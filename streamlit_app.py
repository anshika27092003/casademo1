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
SHEET_ID = "1Iq9jU8QvjF9V0_4_76Rk8V_P_T_P_T_P_T_P_T_P_T" # Placeholder
SETTLEMENT_GID = 305885354
LOCATION = "us"

def get_credentials():
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return Credentials.from_service_account_file("credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])

def get_gsheet_client():
    return gspread.authorize(get_credentials())

def update_google_sheet(amount, category, filename, record_id=None):
    try:
        client = get_gsheet_client()
        sheet = client.open_by_key(SHEET_ID).get_worksheet_by_id(SETTLEMENT_GID)
        
        cell_map = {"CK": "C39", "SP": "C42", "FWL": "C68"}
        cell_ref = cell_map.get(category)
        
        if cell_ref:
            # Atomic Lock to prevent duplicate logging of the sync we are about to do
            db = SessionLocal()
            minute_key = datetime.utcnow().strftime("%Y%m%d%H%M")
            lock_key = f"{cell_ref}_{amount}_{minute_key}"
            try:
                lock = SyncLock(lock_key=lock_key)
                db.add(lock); db.commit()
            except IntegrityError:
                db.rollback(); db.close(); return # Already locked
            
            # Perform update
            sheet.update_acell(cell_ref, amount)
            
            # Update Shared Memory so the tracker doesn't log this as a "manual change"
            state = db.query(SheetState).filter(SheetState.cell_reference == cell_ref).first()
            if state:
                state.last_value = str(amount)
                state.last_updated = datetime.utcnow()
            else:
                db.add(SheetState(cell_reference=cell_ref, last_value=str(amount)))
            
            # Audit log
            label_val = sheet.acell(f"A{cell_ref[1:]}").value
            audit = CellChange(sheet_name="Settlement", cell_reference=cell_ref, label_name=str(label_val), old_value="OCR Sync", new_value=str(amount), source_table=category, source_id=record_id, timestamp=datetime.utcnow())
            db.add(audit); db.commit(); db.close()
            log_to_ui(f"✅ Synced {category} to {cell_ref} (${amount})", type="success")
    except Exception as e:
        log_to_ui(f"❌ Sheet Sync Error: {e}", type="error")

# --- BACKGROUND TRACKER (Shared State) ---
@st.cache_resource
def get_tracker_manager():
    thread = threading.Thread(target=background_polling_loop, daemon=True)
    thread.start()
    return {"status": "running"}

def background_polling_loop():
    logger.info("Background tracker started.")
    while True:
        try:
            db = SessionLocal()
            sheet = get_gsheet_client().open_by_key(SHEET_ID).get_worksheet_by_id(SETTLEMENT_GID)
            # Track C39, C42, C68
            cells = ["C39", "C42", "C68"]
            
            for cell_ref in cells:
                current_val = str(sheet.acell(cell_ref).value or "0").strip()
                
                # Check Shared Memory in Database
                state = db.query(SheetState).filter(SheetState.cell_reference == cell_ref).first()
                if not state:
                    state = SheetState(cell_reference=cell_ref, last_value=current_val)
                    db.add(state); db.commit()
                    continue
                
                if current_val != state.last_value:
                    # WE FOUND A CHANGE!
                    # 1. Atomic Lock (Double protection)
                    lock_key = f"{cell_ref}_{current_val}_{datetime.utcnow().strftime('%Y%m%d%H%M')}"
                    try:
                        lock_entry = SyncLock(lock_key=lock_key)
                        db.add(lock_entry); db.commit()
                    except Exception:
                        db.rollback(); continue
                    
                    # 2. Log change in records
                    row_num = re.findall(r'\d+', cell_ref)[0]
                    label_val = sheet.acell(f"A{row_num}").value
                    source_table, source_id = None, None
                    
                    if cell_ref == "C39":
                        entry = CKSecreterial(filename="Manual Entry", total_amount=current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "CK", entry.id
                    elif cell_ref == "C42":
                        entry = SPTable(filename="Manual Entry", total_amount=current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "SP", entry.id
                    elif cell_ref == "C68":
                        entry = FWLTable(filename="Manual Entry", total_payable=current_val, remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "FWL", entry.id
                    
                    # 3. Audit trail
                    audit = CellChange(sheet_name="Settlement", cell_reference=cell_ref, label_name=str(label_val), old_value=state.last_value, new_value=current_val, source_table=source_table, source_id=source_id, timestamp=datetime.utcnow())
                    db.add(audit)
                    
                    # 4. Update Shared Memory
                    state.last_value = current_val
                    state.last_updated = datetime.utcnow()
                    db.commit()
                    logger.info(f"Recorded manual change in {cell_ref}: {current_val}")
            
            db.close()
            time.sleep(15) 
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(10)

def start_background_tracker():
    get_tracker_manager()

# --- OCR LOGIC ---
from google.cloud import documentai

def get_processor_id():
    return "7303c66f56860f77"

def extract_invoice_data(text, filename=""):
    data = {'total_amount': "Not Found", 'sub_total': "Not Found", 'gst_amount': "0.00", 'remarks': ""}
    category = "CK"
    
    # 1. Determine Category
    if re.search(r"(?i)FWL|Foreign\s+Worker\s+Levy", text) or re.search(r"(?i)FWL", filename):
        category = "FWL"; data['supplier_name'] = "MOM (FWL)"
    elif re.search(r"(?i)Firmus\s+Cap", text) or re.search(r"(?i)SP|Firmus", filename):
        category = "SP"; data['supplier_name'] = "Firmus Cap"
    elif re.search(r"(?i)CK\s+SECRETARIAL", text) or re.search(r"(?i)CK", filename):
        category = "CK"; data['supplier_name'] = "CK SECRETARIAL SERVICES PTE LTD"
    
    # 2. Extract Date
    date_match = re.search(r"(?i)(Date|Dated)\s*[:\s]*([\d\-\/]{6,}|[\d]{1,2}\s+[A-Za-z]{3}\s+[\d]{4})", text)
    data['invoice_date'] = date_match.group(2) if date_match else "Not Found"
    
    # 3. Extract Invoice No
    inv_no_match = re.search(r"(?i)(Invoice|Tax\s+Invoice|Inv)\s+No\.\s*[:\s]*([A-Z0-9\-]+)", text)
    if inv_no_match:
        data['invoice_no'] = inv_no_match.group(2)
    else:
        inv_no_match = re.search(r"(?i)Invoice\s+([A-Z0-9\-]+)", text)
        data['invoice_no'] = inv_no_match.group(1) if inv_no_match else "Not Found"
    
    # 4. Extract Clinic Name
    bill_to_match = re.search(r"(?i)(Bill\s+To|Delivered\s+To|Invoice\s+To|Sold\s+To)\s*[:\s]*([^\n,]+)", text)
    if bill_to_match:
        data['bill_to'] = bill_to_match.group(2).strip()
    else:
        known_search = re.search(r"(?i)(CASA\s+DENTAL\s+[\w\s]+(PTE\s+LTD)?)", text)
        data['bill_to'] = known_search.group(1).strip() if known_search else "Not Found"
    
    # 5. Extract Amounts
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
    
    # 6. Extract Remarks
    remarks_lines = []
    found_particulars = False
    for line in lines:
        if re.search(r"(?i)Particulars|Description|Details|Item", line): found_particulars = True; continue
        if re.search(r"(?i)Sub\s*Total|Total|Payable|Thank\s+You", line): break
        if found_particulars and len(line) > 5: remarks_lines.append(line)
    data['remarks'] = " | ".join(remarks_lines) if remarks_lines else "Not Found"
    
    # 7. --- FIRMUS CAP SPECIALIST ---
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

# --- STREAMLIT UI ---
st.set_page_config(page_title="Casa Dental Hub", page_icon="🦷", layout="wide")
st.title("🦷 Casa Dental - Operations Hub")

# Sidebar Maintenance
with st.sidebar:
    st.header("⚙️ Maintenance")
    if st.button("🧹 Clear All Records", type="secondary"):
        try:
            from database import engine, Base
            Base.metadata.drop_all(bind=engine)
            Base.metadata.create_all(bind=engine)
            st.success("Database cleared!")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

Base.metadata.create_all(bind=engine)
start_background_tracker()

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    st.subheader("🚀 Upload Invoices")
    uploaded_files = st.file_uploader("Drop invoice images here (CK, SP, FWL)", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)
    
    if uploaded_files:
        if st.button("✨ Process All Invoices"):
            sp_batch = []
            st.session_state['pending_fwl'] = []
            
            for uploaded_file in uploaded_files:
                with st.status(f"🔍 Analyzing: {uploaded_file.name}") as status:
                    file_bytes = uploaded_file.read()
                    extracted_text = process_document(file_bytes, uploaded_file.type)
                    if extracted_text:
                        inv_data, category = extract_invoice_data(extracted_text, uploaded_file.name)
                        
                        with st.expander(f"🔍 Debug: Parsed Data for {uploaded_file.name}", expanded=True):
                            st.json(inv_data)
                        with st.expander(f"📄 Full Raw OCR Text for {uploaded_file.name}", expanded=False):
                            st.text(extracted_text)
                        
                        if category == "SP":
                            try:
                                amt_float = float(str(inv_data['total_amount']).replace(",", ""))
                                save_to_db(uploaded_file.name, inv_data, "SP")
                                sp_batch.append(amt_float)
                                status.update(label=f"Added to SP Batch: ${amt_float}", state="complete")
                            except: status.update(label=f"Could not read amount", state="error")
                        elif category == "FWL":
                            st.session_state['pending_fwl'].append({"filename": uploaded_file.name, "data": inv_data})
                            status.update(label=f"Pending Clinic Pickup", state="complete")
                        else:
                            rid = save_to_db(uploaded_file.name, inv_data, category)
                            update_google_sheet(inv_data['total_amount'], category, uploaded_file.name, rid)
                            status.update(label=f"Synced: {uploaded_file.name}", state="complete")
            
            if sp_batch:
                st.session_state['sp_batch_total'] = sum(sp_batch)
                st.session_state['sp_batch_count'] = len(sp_batch)

        if st.session_state.get('pending_fwl'):
            st.divider(); st.subheader("🏥 Pending FWL Confirmation")
            for i, item in enumerate(st.session_state['pending_fwl']):
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1: st.write(f"📄 {item['filename']} (**${item['data']['total_amount']}**)")
                with col2: 
                    clinic = st.selectbox(f"Assign Clinic for {item['filename']}", ["ADMIRALTY", "AMK", "BATOK", "CLEMENTI", "HOLLAND", "JURONG", "KAMPUNG", "TAMPINES"], key=f"fwl_{i}")
                with col3:
                    if st.button(f"Sync FWL #{i+1}"):
                        item['data']['bill_to'] = f"CASA DENTAL {clinic} PTE LTD"
                        rid = save_to_db(item['filename'], item['data'], "FWL")
                        update_google_sheet(item['data']['total_amount'], "FWL", item['filename'], rid)
                        st.session_state['pending_fwl'].pop(i); st.rerun()

        if 'sp_batch_total' in st.session_state:
            st.divider(); st.subheader("📊 SP Batch Ready")
            st.info(f"Total Sum: **${st.session_state['sp_batch_total']:.2f}** ({st.session_state['sp_batch_count']} files)")
            if st.button("🚀 Sync SP Total to C42"):
                update_google_sheet(f"{st.session_state['sp_batch_total']:.2f}", "SP", "Batch Update")
                del st.session_state['sp_batch_total']; st.balloons()

with tab2:
    st.subheader("📊 Audit Trail & Records")
    
    # CK Table
    st.markdown("### 📝 CK Secreterial (C39)")
    try:
        db = SessionLocal()
        results = db.query(CKSecreterial).order_by(CKSecreterial.timestamp.desc()).all()
        db.close()
        if results:
            data = [{
                "ID": r.id,
                "Filename": r.filename,
                "Supplier": r.supplier_name,
                "Consignment No": r.consignment_number,
                "Invoice Date": r.invoice_date,
                "Invoice No": r.invoice_no,
                "Bill To": r.bill_to,
                "Sub Total": f"${r.sub_total}" if r.sub_total else "N/A",
                "GST Amount": f"${r.gst_amount}" if r.gst_amount else "N/A",
                "Total Amount": f"${r.total_amount}",
                "Remarks": r.remarks
            } for r in results]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else: st.info("No CK records found.")
    except Exception as e: st.error(str(e))

    # SP Table
    st.markdown("### ⚡ SP - Firmus Cap (C42)")
    try:
        db = SessionLocal()
        results = db.query(SPTable).order_by(SPTable.timestamp.desc()).all()
        db.close()
        if results:
            data = [{
                "ID": r.id,
                "Supplier": r.supplier_name,
                "Clinic": r.clinic_name,
                "Date": r.invoice_date,
                "Inv No": r.tax_invoice_number,
                "Sub Total": f"${r.sub_total}" if r.sub_total else "N/A",
                "GST": f"${r.gst_amount}" if r.gst_amount else "N/A",
                "Total": f"${r.total_amount}",
                "Remarks": r.remarks
            } for r in results]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else: st.info("No SP records found.")
    except Exception as e: st.error(str(e))

    # FWL Table
    st.markdown("### 🏢 FWL - Foreign Worker Levy (C68)")
    try:
        db = SessionLocal()
        results = db.query(FWLTable).order_by(FWLTable.timestamp.desc()).all()
        db.close()
        if results:
            data = [{
                "ID": r.id,
                "Clinic": r.clinic_name,
                "Total": f"${r.total_payable}",
                "Time": r.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            } for r in results]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else: st.info("No FWL records found.")
    except Exception as e: st.error(str(e))

    # Full Audit Trail
    st.divider(); st.markdown("### 🔍 Full Audit Trail")
    try:
        db = SessionLocal()
        logs = db.query(CellChange).order_by(CellChange.timestamp.desc()).all()
        db.close()
        if logs:
            audit_data = [{
                "Table": l.source_table or "Manual",
                "Record ID": l.source_id or "-",
                "Cell": l.cell_reference,
                "Label": l.label_name,
                "Old": l.old_value,
                "New": l.new_value,
                "Time": l.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            } for l in logs]
            st.dataframe(pd.DataFrame(audit_data), use_container_width=True)
        else: st.info("No audit logs yet.")
    except Exception as e: st.error(str(e))
