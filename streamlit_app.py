import streamlit as st
import pandas as pd
import os
import re
import time
from datetime import datetime
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base, CKSecreterial, SPTable, FWLTable, CellChange, SyncLock, SheetState
import gspread
from google.oauth2.service_account import Credentials
import logging
from sqlalchemy.exc import IntegrityError
import threading

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_to_ui(msg, type="info"):
    if type == "error": st.error(msg)
    elif type == "success": st.success(msg)
    else: st.info(msg)

# --- GOOGLE SHEETS SETUP ---
SHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10" 
SETTLEMENT_GID = 305885354
LOCATION = "us"

def get_credentials():
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return Credentials.from_service_account_file("credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])

def get_gsheet_client():
    return gspread.authorize(get_credentials())

def parse_amount(value):
    if value is None:
        return 0.0
    txt = str(value).strip().replace(",", "")
    if not txt:
        return 0.0
    try:
        return float(txt)
    except ValueError:
        return 0.0

def format_amount(value):
    return f"{float(value):.2f}"

def is_duplicate_manual_change(db: Session, cell_ref: str, new_val: str) -> bool:
    """Guard against duplicate inserts for the same manual sheet edit."""
    last_change = (
        db.query(CellChange)
        .filter(CellChange.cell_reference == cell_ref)
        .order_by(CellChange.timestamp.desc())
        .first()
    )
    if not last_change:
        return False

    # If the latest log already has the same target value, skip duplicate logging.
    return str(last_change.new_value).strip() == str(new_val).strip()

def update_google_sheet(amount, category, filename, record_id=None):
    try:
        client = get_gsheet_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        settlement = spreadsheet.get_worksheet_by_id(SETTLEMENT_GID)
        
        cell_map = {"CK": "C39", "SP": "C42", "FWL": "C68"}
        cell_ref = cell_map.get(category)
        
        if cell_ref:
            final_amount = amount
            if category == "CK":
                current_val = settlement.acell(cell_ref).value
                final_amount = format_amount(parse_amount(current_val) + parse_amount(amount))

            # Update the Shared Brain in the sheet so the tracker knows this was us
            try:
                sync_sheet = spreadsheet.worksheet("_SYNC_STATE_")
                cell_list = sync_sheet.col_values(1)
                row_idx = cell_list.index(cell_ref) + 1
                sync_sheet.update_cell(row_idx, 2, str(final_amount))
            except: pass 
            
            # Perform update
            settlement.update_acell(cell_ref, final_amount)
            
            # Audit log
            db = SessionLocal()
            label_val = settlement.acell(f"A{cell_ref[1:]}").value
            audit = CellChange(sheet_name="Settlement", cell_reference=cell_ref, label_name=str(label_val), old_value="OCR Upload", new_value=str(final_amount), source_table=category, source_id=record_id, timestamp=datetime.utcnow())
            db.add(audit); db.commit(); db.close()
            log_to_ui(f"✅ Synced {category} to {cell_ref} (${final_amount})", type="success")
    except Exception as e:
        log_to_ui(f"❌ Sheet Sync Error: {e}", type="error")

def save_to_db(filename, data, category):
    db = SessionLocal()
    try:
        if category == "CK":
            entry = CKSecreterial(
                filename=filename,
                supplier_name=data.get('supplier_name'),
                invoice_date=data.get('invoice_date'),
                invoice_no=data.get('invoice_no'),
                bill_to=data.get('bill_to'),
                total_amount=data.get('total_amount'),
                remarks=data.get('remarks'),
                timestamp=datetime.utcnow()
            )
        elif category == "SP":
            entry = SPTable(
                filename=filename,
                supplier_name=data.get('supplier_name'),
                clinic_name=data.get('bill_to'),
                invoice_date=data.get('invoice_date'),
                tax_invoice_number=data.get('invoice_no'),
                sub_total=data.get('sub_total'),
                gst_amount=data.get('gst_amount'),
                total_amount=data.get('total_amount'),
                remarks=data.get('remarks'),
                timestamp=datetime.utcnow()
            )
        elif category == "FWL":
            entry = FWLTable(
                filename=filename,
                clinic_name=data.get('bill_to'),
                total_payable=data.get('total_amount'),
                remarks=data.get('remarks'),
                timestamp=datetime.utcnow()
            )
        
        db.add(entry)
        db.commit()
        db.refresh(entry)
        return entry.id
    except Exception as e:
        db.rollback()
        logger.error(f"DB Save Error: {e}")
        return None
    finally:
        db.close()

# --- BACKGROUND TRACKER ---
@st.cache_resource
def get_tracker_manager():
    thread = threading.Thread(target=background_polling_loop, daemon=True)
    thread.start()
    return {"status": "running"}

def background_polling_loop():
    logger.info("Background tracker started.")
    while True:
        try:
            client = get_gsheet_client()
            spreadsheet = client.open_by_key(SHEET_ID)
            settlement = spreadsheet.get_worksheet_by_id(SETTLEMENT_GID)
            
            try:
                sync_sheet = spreadsheet.worksheet("_SYNC_STATE_")
            except gspread.WorksheetNotFound:
                sync_sheet = spreadsheet.add_worksheet(title="_SYNC_STATE_", rows="100", cols="2")
                sync_sheet.update("A1:B1", [["Cell", "LastValue"]])
                sync_sheet.update("A2:B4", [["C39", "0"], ["C42", "0"], ["C68", "0"]])
            
            cells = ["C39", "C42", "C68"]
            sync_data = sync_sheet.get_all_records()
            shared_memory = {str(r['Cell']): str(r['LastValue']).strip() for r in sync_data}
            
            for cell_ref in cells:
                current_val = str(settlement.acell(cell_ref).value or "0").strip()
                last_logged_val = shared_memory.get(cell_ref, "0")
                
                if current_val != last_logged_val:
                    # 1. Update Sheet Sync State FIRST (Locks other workers)
                    cell_list = sync_sheet.col_values(1)
                    try:
                        row_idx = cell_list.index(cell_ref) + 1
                        sync_sheet.update_cell(row_idx, 2, current_val)
                    except ValueError:
                        sync_sheet.append_row([cell_ref, current_val])
                    
                    # 2. Log to DB
                    db = SessionLocal()
                    row_num = re.findall(r'\d+', cell_ref)[0]
                    label_val = settlement.acell(f"A{row_num}").value
                    normalized_current_val = format_amount(parse_amount(current_val))
                    normalized_last_logged_val = format_amount(parse_amount(last_logged_val))

                    if is_duplicate_manual_change(db, cell_ref, normalized_current_val):
                        db.close()
                        logger.info(f"Skipped duplicate manual change for {cell_ref} -> {normalized_current_val}")
                        continue
                    
                    source_table, source_id = None, None
                    if cell_ref == "C39":
                        entry = CKSecreterial(filename="Manual Entry", total_amount=normalized_current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "CK", entry.id
                    elif cell_ref == "C42":
                        entry = SPTable(filename="Manual Entry", total_amount=normalized_current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "SP", entry.id
                    elif cell_ref == "C68":
                        entry = FWLTable(filename="Manual Entry", total_payable=normalized_current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "FWL", entry.id
                    
                    audit = CellChange(sheet_name="Settlement", cell_reference=cell_ref, label_name=str(label_val), old_value=normalized_last_logged_val, new_value=normalized_current_val, source_table=source_table, source_id=source_id, timestamp=datetime.utcnow())
                    db.add(audit); db.commit(); db.close()
                    
                    logger.info(f"SUCCESS: Recorded manual change in {cell_ref} as {normalized_current_val}")
            
            time.sleep(15) 
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(10)

def start_background_tracker():
    get_tracker_manager()

# --- OCR ENGINE ---
from google.cloud import documentai
def get_processor_id(): return "7303c66f56860f77"

def extract_invoice_data(text, filename=""):
    data = {'total_amount': "Not Found", 'sub_total': "Not Found", 'gst_amount': "0.00", 'remarks': ""}
    category = "CK"
    
    if re.search(r"(?i)FWL|Foreign\s+Worker\s+Levy", text) or re.search(r"(?i)FWL", filename):
        category = "FWL"; data['supplier_name'] = "MOM (FWL)"
    elif re.search(r"(?i)Firmus\s+Cap", text) or re.search(r"(?i)SP|Firmus", filename):
        category = "SP"; data['supplier_name'] = "Firmus Cap"
    elif re.search(r"(?i)CK\s+SECRETARIAL", text) or re.search(r"(?i)CK", filename):
        category = "CK"; data['supplier_name'] = "CK SECRETARIAL SERVICES PTE LTD"
    
    date_match = re.search(r"(?i)(Date|Dated)\s*[:\s]*([\d\-\/]{6,}|[\d]{1,2}\s+[A-Za-z]{3}\s+[\d]{4})", text)
    data['invoice_date'] = date_match.group(2) if date_match else "Not Found"
    
    inv_no_match = re.search(r"(?i)(Invoice|Tax\s+Invoice|Inv)\s+No\.\s*[:\s]*([A-Z0-9\-]+)", text)
    if inv_no_match:
        data['invoice_no'] = inv_no_match.group(2)
    else:
        inv_no_match = re.search(r"(?i)Invoice\s+([A-Z0-9\-]+)", text)
        data['invoice_no'] = inv_no_match.group(1) if inv_no_match else "Not Found"
    
    bill_to_match = re.search(r"(?i)(Bill\s+To|Delivered\s+To|Invoice\s+To|Sold\s+To)\s*[:\s]*([^\n,]+)", text)
    if bill_to_match:
        data['bill_to'] = bill_to_match.group(2).strip()
    else:
        known_search = re.search(r"(?i)(CASA\s+DENTAL\s+[\w\s]+(PTE\s+LTD)?)", text)
        data['bill_to'] = known_search.group(1).strip() if known_search else "Not Found"
    
    all_amounts = re.findall(r"[\d,]+\.\d{2}", text)
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    for i, line in enumerate(lines):
        if re.search(r"(?i)\b(Total|Grand\s*Total|Total\s*Payable|Amount\s*Due)\b", line) and not re.search(r"(?i)sub", line):
            amt = re.search(r"[\d,]+\.\d{2}", line)
            if not amt and i+1 < len(lines): amt = re.search(r"[\d,]+\.\d{2}", lines[i+1])
            if amt: data['total_amount'] = amt.group(0).replace(",", "")
            break
    if data['total_amount'] == "Not Found" and all_amounts:
        data['total_amount'] = all_amounts[-1].replace(",", "")
    
    remarks_lines = []
    found_particulars = False
    for line in lines:
        if re.search(r"(?i)Particulars|Description|Details|Item", line): found_particulars = True; continue
        if re.search(r"(?i)Sub\s*Total|Total|Payable|Thank\s+You", line): break
        if found_particulars and len(line) > 5: remarks_lines.append(line)
    data['remarks'] = " | ".join(remarks_lines) if remarks_lines else "Not Found"
    
    if "Firmus" in str(data.get('supplier_name', '')) or "Firmus" in text or "SP" in filename:
        data['supplier_name'] = "FIRMUS CAP (BBCR) PTE LTD"
        m_clinic = re.search(r"(?i)CASA\s+DENTAL\s+\(?([^\)\n]+)\)?\s+PTE\s+LTD", text)
        if m_clinic: data['bill_to'] = f"CASA DENTAL ({m_clinic.group(1).strip()}) PTE LTD"
        m_no = re.search(r"(?i)Tax\s+Invoice\s+No\s*:\s*([A-Z0-9]+)", text)
        if m_no: data['invoice_no'] = m_no.group(1).strip()
        m_date = re.search(r"(?i)Tax\s+Invoice\s+Date\s*:\s*([\d\/]+)", text)
        if m_date: data['invoice_date'] = m_date.group(1).strip()
        m_sub = re.search(r"(?i)Sub\s+Total\s*:\s*([\d,]+\.\d{2})", text)
        if m_sub: data['sub_total'] = m_sub.group(1).replace(",", "")
        m_total = re.search(r"(?i)Grand\s+Total\s*:\s*([\d,]+\.\d{2})", text)
        if m_total: data['total_amount'] = m_total.group(1).replace(",", "")

    return data, category

def process_document(file_content, mime_type):
    try:
        credentials = get_credentials()
        client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}, credentials=credentials)
        project_id = "casa-dental-ops"
        name = client.processor_path(project_id, LOCATION, get_processor_id())
        raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)
        return client.process_document(request=request).document.text
    except Exception as e: log_to_ui(f"ERROR: {str(e)}", type="error"); return None

# --- STREAMLIT PAGE ---
st.set_page_config(page_title="Casa Dental Hub", page_icon="🦷", layout="wide")
st.title("🦷 Casa Dental - Operations Hub")

with st.sidebar:
    st.header("⚙️ Maintenance")
    if st.button("清理数据库 (Clear All)", type="secondary"):
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        st.success("Re-initialized!")
        st.rerun()

Base.metadata.create_all(bind=engine)
start_background_tracker()

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    st.subheader("🚀 Upload Invoices")
    uploaded_files = st.file_uploader("Drop invoice images", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)
    
    if uploaded_files:
        if st.button("✨ Process All"):
            sp_batch = []
            st.session_state['pending_fwl'] = []
            for f in uploaded_files:
                with st.status(f"Processing {f.name}"):
                    text = process_document(f.read(), f.type)
                    if text:
                        inv, cat = extract_invoice_data(text, f.name)
                        st.json(inv)
                        with st.expander("📄 Raw Text"): st.text(text)
                        
                        if cat == "SP":
                            save_to_db(f.name, inv, "SP")
                            sp_batch.append(float(str(inv['total_amount']).replace(",", "")))
                        elif cat == "FWL":
                            st.session_state['pending_fwl'].append({"filename": f.name, "data": inv})
                        else:
                            rid = save_to_db(f.name, inv, cat)
                            update_google_sheet(inv['total_amount'], cat, f.name, rid)

            if sp_batch:
                st.session_state['sp_total'] = sum(sp_batch)
                st.session_state['sp_count'] = len(sp_batch)

        if st.session_state.get('pending_fwl'):
            for i, item in enumerate(st.session_state['pending_fwl']):
                clinic = st.selectbox(f"Clinic for {item['filename']}", ["ADMIRALTY", "AMK", "BATOK", "CLEMENTI", "HOLLAND", "JURONG", "KAMPUNG", "TAMPINES"], key=f"fwl_{i}")
                if st.button(f"Sync {item['filename']}"):
                    item['data']['bill_to'] = clinic
                    rid = save_to_db(item['filename'], item['data'], "FWL")
                    update_google_sheet(item['data']['total_amount'], "FWL", item['filename'], rid)
                    st.session_state['pending_fwl'].pop(i); st.rerun()

        if 'sp_total' in st.session_state:
            st.success(f"Batch Total: ${st.session_state['sp_total']:.2f}")
            if st.button("Confirm SP Sync"):
                update_google_sheet(f"{st.session_state['sp_total']:.2f}", "SP", "Batch")
                del st.session_state['sp_total']; st.rerun()

with tab2:
    st.subheader("📋 Audit Trail")
    def show(title, model, map_fn):
        st.write(f"### {title}")
        db = SessionLocal()
        rows = db.query(model).order_by(model.timestamp.desc()).all()
        db.close()
        if rows: st.dataframe(pd.DataFrame([map_fn(r) for r in rows]), use_container_width=True)

    show("CK", CKSecreterial, lambda r: {"File": r.filename, "Date": r.invoice_date, "Amt": r.total_amount, "Time": r.timestamp.strftime("%H:%M")})
    show("SP", SPTable, lambda r: {"File": r.filename, "Clinic": r.clinic_name, "Amt": r.total_amount})
    show("FWL", FWLTable, lambda r: {"File": r.filename, "Clinic": r.clinic_name, "Amt": r.total_payable})
    
    st.write("### 🔍 System Logs")
    db = SessionLocal()
    logs = db.query(CellChange).order_by(CellChange.timestamp.desc()).all()
    db.close()
    if logs: st.dataframe(pd.DataFrame([{"Cell": l.cell_reference, "Old": l.old_value, "New": l.new_value, "Time": l.timestamp.strftime("%H:%M:%S")} for l in logs]), use_container_width=True)
