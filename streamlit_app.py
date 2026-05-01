import streamlit as st
import pandas as pd
import os
import json
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
CK_STATIC_COLUMNS = [
    "filename",
    "supplier_name",
    "consignment_number",
    "invoice_date",
    "invoice_no",
    "bill_to",
    "sub_total",
    "gst_amount",
    "total_amount",
    "remarks",
]

def _load_service_account_info():
    try:
        service_account_info = st.secrets.get("gcp_service_account", None)
        if service_account_info:
            return service_account_info
    except Exception as e:
        logger.warning(f"Streamlit secrets unavailable, falling back to credentials.json: {e}")
    return None

def get_sheet_credentials():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    service_account_info = _load_service_account_info()
    if service_account_info:
        return Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return Credentials.from_service_account_file("credentials.json", scopes=scopes)

def get_documentai_credentials():
    # Document AI needs cloud-platform scope. Using sheet scopes can return 401.
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    service_account_info = _load_service_account_info()
    if service_account_info:
        return Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return Credentials.from_service_account_file("credentials.json", scopes=scopes)

def get_gsheet_client():
    return gspread.authorize(get_sheet_credentials())

def normalize_ck_payload(filename, data):
    """Keep CK data aligned to a strict, static CK table schema."""
    payload = {
        "filename": filename,
        "supplier_name": "Not Found",
        "consignment_number": "Not Found",
        "invoice_date": "Not Found",
        "invoice_no": "Not Found",
        "bill_to": "Not Found",
        "sub_total": "Not Found",
        "gst_amount": "0.00",
        "total_amount": "Not Found",
        "remarks": "Not Found",
    }
    for key in payload:
        if key == "filename":
            continue
        value = data.get(key) if isinstance(data, dict) else None
        if value not in (None, ""):
            payload[key] = str(value)
    return {k: payload[k] for k in CK_STATIC_COLUMNS}

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
            
            # Perform update
            settlement.update_acell(cell_ref, final_amount)
            
            # Audit log
            db = SessionLocal()
            state = db.query(SheetState).filter(SheetState.cell_reference == cell_ref).first()
            if not state:
                state = SheetState(cell_reference=cell_ref, last_value=str(final_amount), last_updated=datetime.utcnow())
                db.add(state)
            else:
                state.last_value = str(final_amount)
                state.last_updated = datetime.utcnow()

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
            ck_data = normalize_ck_payload(filename, data)
            entry = CKSecreterial(
                filename=ck_data.get('filename'),
                supplier_name=ck_data.get('supplier_name'),
                consignment_number=ck_data.get('consignment_number'),
                invoice_date=ck_data.get('invoice_date'),
                invoice_no=ck_data.get('invoice_no'),
                bill_to=ck_data.get('bill_to'),
                sub_total=ck_data.get('sub_total'),
                gst_amount=ck_data.get('gst_amount'),
                total_amount=ck_data.get('total_amount'),
                remarks=ck_data.get('remarks'),
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
        sync_sheet_changes_once()
        time.sleep(15)

def sync_sheet_changes_once():
    cells = ["C39", "C42", "C68"]
    db = None
    try:
        client = get_gsheet_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        settlement = spreadsheet.get_worksheet_by_id(SETTLEMENT_GID)
        db = SessionLocal()

        for cell_ref in cells:
            current_val = format_amount(parse_amount(settlement.acell(cell_ref).value or "0"))
            state = db.query(SheetState).filter(SheetState.cell_reference == cell_ref).first()
            if not state:
                db.add(SheetState(cell_reference=cell_ref, last_value=current_val, last_updated=datetime.utcnow()))
                db.commit()
                continue

            last_logged_val = format_amount(parse_amount(state.last_value))
            if current_val != last_logged_val:
                row_num = re.findall(r'\d+', cell_ref)[0]
                label_val = settlement.acell(f"A{row_num}").value
                normalized_current_val = current_val
                normalized_last_logged_val = last_logged_val

                if is_duplicate_manual_change(db, cell_ref, normalized_current_val):
                    state.last_value = normalized_current_val
                    state.last_updated = datetime.utcnow()
                    db.commit()
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
                state.last_value = normalized_current_val
                state.last_updated = datetime.utcnow()
                db.add(audit); db.commit()
                
                logger.info(f"SUCCESS: Recorded manual change in {cell_ref} as {normalized_current_val}")
    except Exception as e:
        logger.error(f"Polling error: {e}")
    finally:
        if db is not None:
            db.close()

def start_background_tracker():
    get_tracker_manager()

# --- OCR ENGINE ---
from google.cloud import documentai
def get_processor_id():
    try:
        service_account_info = st.secrets.get("gcp_service_account", None)
        if isinstance(service_account_info, dict) and service_account_info.get("processor_id"):
            return str(service_account_info["processor_id"])
    except Exception:
        pass

    credentials_path = "credentials.json"
    if os.path.exists(credentials_path):
        try:
            with open(credentials_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if payload.get("processor_id"):
                return str(payload["processor_id"])
        except Exception as e:
            logger.warning(f"Unable to parse processor_id from credentials.json: {e}")

    return "7303c66f56860f77"

def extract_invoice_data(text, filename=""):
    data = {'total_amount': "Not Found", 'sub_total': "Not Found", 'gst_amount': "0.00", 'remarks': ""}
    category = "CK"
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    
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
        # CK invoices often include clinic name on a standalone line (e.g. "... DENTAL ... PTE LTD").
        bill_to_line = None
        for line in lines:
            if re.search(r"(?i)\bDENTAL\b.*\bPTE\s*LTD\b", line):
                cleaned = re.sub(r"^[\d\W_]+", "", line).strip()
                bill_to_line = re.sub(r"\s+", " ", cleaned)
                break

        if bill_to_line:
            data['bill_to'] = bill_to_line
        else:
            known_search = re.search(r"(?i)(CASA\s+DENTAL\s+[\w\s]+(PTE\s+LTD)?)", text)
            data['bill_to'] = known_search.group(1).strip() if known_search else "Not Found"
    
    all_amounts = re.findall(r"[\d,]+\.\d{2}", text)
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
        credentials = get_documentai_credentials()
        client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}, credentials=credentials)
        project_id = "casa-dental-ops"
        name = client.processor_path(project_id, LOCATION, get_processor_id())
        raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)
        text = client.process_document(request=request).document.text
        return text, None
    except Exception as e:
        logger.exception("OCR processing failed")
        log_to_ui(f"ERROR: {str(e)}", type="error")
        return None, str(e)

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
# Run one foreground sync each rerun so manual sheet edits are visible without
# relying only on background thread scheduling.
sync_sheet_changes_once()
start_background_tracker()

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    st.subheader("🚀 Upload Invoices")
    uploaded_files = st.file_uploader("Drop invoice images", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)
    
    if uploaded_files:
        if 'ocr_preview' not in st.session_state:
            st.session_state['ocr_preview'] = {}
        if 'auto_processed_ck' not in st.session_state:
            st.session_state['auto_processed_ck'] = set()

        current_keys = set()
        for f in uploaded_files:
            file_bytes = f.getvalue()
            file_key = f"{f.name}:{len(file_bytes)}"
            current_keys.add(file_key)

            if file_key not in st.session_state['ocr_preview']:
                with st.status(f"Extracting OCR for {f.name}..."):
                    text, err = process_document(file_bytes, f.type)
                    if text:
                        inv, cat = extract_invoice_data(text, f.name)
                        if cat == "CK":
                            inv = normalize_ck_payload(f.name, inv)
                        st.session_state['ocr_preview'][file_key] = {
                            "filename": f.name,
                            "mime_type": f.type,
                            "data": inv,
                            "category": cat,
                            "text": text,
                            "error": None,
                        }
                    else:
                        st.session_state['ocr_preview'][file_key] = {
                            "filename": f.name,
                            "mime_type": f.type,
                            "data": None,
                            "category": None,
                            "text": None,
                            "error": err or "Unknown OCR error",
                        }

        # Remove previews for files that are no longer selected.
        st.session_state['ocr_preview'] = {
            k: v for k, v in st.session_state['ocr_preview'].items() if k in current_keys
        }
        st.session_state['auto_processed_ck'] = {
            k for k in st.session_state['auto_processed_ck'] if k in current_keys
        }

        # Auto flow for CK: save to CK table first, then sync stored total to C39.
        for file_key, preview in st.session_state['ocr_preview'].items():
            if file_key in st.session_state['auto_processed_ck']:
                continue
            if preview.get("category") != "CK" or not preview.get("data"):
                continue

            with st.status(f"Auto-saving CK: {preview['filename']}"):
                rid = save_to_db(preview["filename"], preview["data"], "CK")
                if rid:
                    db = SessionLocal()
                    ck_row = db.query(CKSecreterial).filter(CKSecreterial.id == rid).first()
                    db.close()
                    if ck_row:
                        update_google_sheet(ck_row.total_amount, "CK", preview["filename"], rid)
                        st.session_state['auto_processed_ck'].add(file_key)
                        preview["auto_status"] = "Saved to CK and synced to C39"
                    else:
                        preview["auto_status"] = "Saved failed: CK row not found after insert"
                else:
                    preview["auto_status"] = "Save failed: DB insert error"

        st.write("### 👀 OCR Preview")
        for _, preview in st.session_state['ocr_preview'].items():
            st.write(f"**{preview['filename']}**")
            if preview["data"] is None:
                st.error("OCR failed for this file.")
                st.caption(f"Reason: {preview.get('error', 'Unknown error')}")
                continue
            st.caption(f"Detected Category: {preview['category']}")
            if preview.get("auto_status"):
                st.caption(f"Status: {preview['auto_status']}")
            st.json(preview["data"])
            with st.expander("📄 Raw OCR Text"):
                st.text(preview["text"])

        if st.button("✨ Process All"):
            sp_batch = []
            st.session_state['pending_fwl'] = []
            for _, preview in st.session_state.get('ocr_preview', {}).items():
                if not preview.get("data"):
                    continue

                f_name = preview["filename"]
                inv = preview["data"]
                cat = preview["category"]

                with st.status(f"Processing {f_name}"):
                    if cat == "SP":
                        save_to_db(f_name, inv, "SP")
                        sp_batch.append(float(str(inv['total_amount']).replace(",", "")))
                    elif cat == "FWL":
                        st.session_state['pending_fwl'].append({"filename": f_name, "data": inv})
                    elif cat != "CK":
                        rid = save_to_db(f_name, inv, cat)
                        update_google_sheet(inv['total_amount'], cat, f_name, rid)

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
    CK_VIEW_COLUMNS = [
        "filename",
        "supplier_name",
        "consignment_number",
        "invoice_date",
        "invoice_no",
        "bill_to",
        "sub_total",
        "gst_amount",
        "total_amount",
        "remarks",
        "timestamp",
    ]

    def map_ck_row(r):
        return {
            "filename": r.filename or "Not Found",
            "supplier_name": r.supplier_name or "Not Found",
            "consignment_number": r.consignment_number or "Not Found",
            "invoice_date": r.invoice_date or "Not Found",
            "invoice_no": r.invoice_no or "Not Found",
            "bill_to": r.bill_to or "Not Found",
            "sub_total": r.sub_total or "Not Found",
            "gst_amount": r.gst_amount or "0.00",
            "total_amount": r.total_amount or "Not Found",
            "remarks": r.remarks or "Not Found",
            "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if r.timestamp else "",
        }

    def show(title, model, map_fn):
        st.write(f"### {title}")
        db = SessionLocal()
        rows = db.query(model).order_by(model.timestamp.desc()).all()
        db.close()
        if rows: st.dataframe(pd.DataFrame([map_fn(r) for r in rows]), use_container_width=True)

    st.write("### CK")
    db = SessionLocal()
    ck_rows = db.query(CKSecreterial).order_by(CKSecreterial.timestamp.desc()).all()
    db.close()
    if ck_rows:
        ck_df = pd.DataFrame([map_ck_row(r) for r in ck_rows], columns=CK_VIEW_COLUMNS)
        st.dataframe(ck_df, use_container_width=True)

    show("SP", SPTable, lambda r: {"File": r.filename, "Clinic": r.clinic_name, "Amt": r.total_amount})
    show("FWL", FWLTable, lambda r: {"File": r.filename, "Clinic": r.clinic_name, "Amt": r.total_payable})
    
    st.write("### 🔍 System Logs")
    db = SessionLocal()
    logs = db.query(CellChange).order_by(CellChange.timestamp.desc()).all()
    db.close()
    if logs: st.dataframe(pd.DataFrame([{"Cell": l.cell_reference, "Old": l.old_value, "New": l.new_value, "Time": l.timestamp.strftime("%H:%M:%S")} for l in logs]), use_container_width=True)
