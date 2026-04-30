import requests
import csv
import time
import logging
import threading
import json
import re
import pandas as pd
import gspread
from io import BytesIO
from datetime import datetime
import streamlit as st
from google.cloud import documentai_v1 as documentai
from google.oauth2 import service_account
from database import SessionLocal, CKSecreterial, SPTable, FWLTable, CellChange, Base, engine

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
LOCATION = "us" 
SPREADSHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10"
GOOGLE_SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid=305885354"

CLINICS = [
    "CASA DENTAL AMK PTE LTD", "CASA DENTAL CLEMENTI PTE LTD",
    "CASA DENTAL HOLLAND PTE LTD", "CASA DENTAL ADM PTE LTD",
    "CASA DENTAL BB PTE LTD", "CASA DENTAL WDLS PTE LTD"
]

def get_credentials():
    """Fetches GCP credentials from Streamlit secrets (cloud) or local file (dev)."""
    if "gcp_service_account" in st.secrets:
        return service_account.Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]))
    else:
        return service_account.Credentials.from_service_account_file("credentials.json")

def get_processor_id():
    """Fetches Document AI Processor ID from secrets or local file."""
    if "gcp_service_account" in st.secrets:
        return st.secrets["gcp_service_account"]["processor_id"]
    else:
        with open("credentials.json", "r") as f: return json.load(f)["processor_id"]

def log_to_ui(message, type="info"):
    logger.info(message)
    if type == "info": st.info(message)
    elif type == "success": st.success(message)
    elif type == "error": st.error(message)
    elif type == "warning": st.warning(message)

# --- TRACKER LOGIC ---
def get_column_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def load_google_sheet_state(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content.decode('utf-8')
            csv_reader = csv.reader(content.splitlines())
            rows = list(csv_reader)
            sheet_data = {}
            for r_idx, row in enumerate(rows):
                for c_idx, val in enumerate(row):
                    if val.strip():
                        cell_ref = f"{get_column_letter(c_idx+1)}{r_idx+1}"
                        sheet_data[cell_ref] = val.strip()
            return sheet_data
    except Exception as e: logger.error(f"Error loading Google Sheet: {e}")
    return {}

@st.cache_resource
def get_tracker_manager():
    """Creates a singleton manager to ensure only one tracker thread runs globally."""
    class TrackerManager:
        def __init__(self):
            self.thread = None
            self.running = False

        def start(self):
            if not self.running:
                self.running = True
                self.thread = threading.Thread(target=background_polling_loop, daemon=True)
                self.thread.start()
                logger.info("Master Singleton Tracker started.")

    manager = TrackerManager()
    manager.start()
    return manager

def background_polling_loop():
    # Load initial state
    last_state = load_google_sheet_state(GOOGLE_SHEET_CSV_URL)
    while True:
        try:
            time.sleep(15) # Poll every 15s for stability
            new_state = load_google_sheet_state(GOOGLE_SHEET_CSV_URL)
            if not new_state: continue
            
            db = SessionLocal()
            changes_found = False
            
            # Identify monitored cells
            for cell_ref in ["C39", "C42", "C68"]:
                new_val = new_state.get(cell_ref)
                old_val = last_state.get(cell_ref)
                
                # Only log if value actually changed and we have both values
                if old_val and new_val and new_val != old_val:
                    row_num = re.findall(r'\d+', cell_ref)[0]
                    label_val = new_state.get(f"A{row_num}", "Manual Update")
                    
                    source_table, source_id = None, None
                    if cell_ref == "C39":
                        entry = CKSecreterial(filename="Manual Entry", total_amount=str(new_val), remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "CK", entry.id
                    elif cell_ref == "C42":
                        entry = SPTable(filename="Manual Entry", total_amount=str(new_val), remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "SP", entry.id
                    elif cell_ref == "C68":
                        entry = FWLTable(filename="Manual Entry", total_payable=str(new_val), remarks="Manual edit in Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_table, source_id = "FWL", entry.id
                    
                    audit = CellChange(sheet_name="Settlement Sheet", cell_reference=cell_ref, label_name=str(label_val), old_value=str(old_val), new_value=str(new_val), source_table=source_table, source_id=source_id, timestamp=datetime.utcnow())
                    db.add(audit); changes_found = True
                    logger.info(f"Detected change in {cell_ref}: {old_val} -> {new_val}")
            
            if changes_found: db.commit()
            db.close()
            last_state = new_state # Update memory AFTER processing
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(10)

def start_background_tracker():
    # This now just triggers the cached resource
    get_tracker_manager()


# --- DATABASE LOGIC ---
def save_to_db(filename, data, category):
    try:
        db = SessionLocal()
        if category == "CK":
            new_entry = CKSecreterial(filename=filename, supplier_name=data.get('supplier_name'), consignment_number=data.get('consignment_number'), invoice_date=data.get('invoice_date'), invoice_no=data.get('invoice_no'), bill_to=data.get('bill_to'), sub_total=data.get('sub_total'), gst_amount=data.get('gst_amount'), total_amount=str(data.get('total_amount')), remarks=data.get('remarks'), timestamp=datetime.utcnow())
        elif category == "SP":
            new_entry = SPTable(filename=filename, supplier_name=data.get('supplier_name', "Firmus Cap"), clinic_name=data.get('bill_to'), invoice_date=data.get('invoice_date'), tax_invoice_number=data.get('invoice_no'), sub_total=data.get('sub_total'), gst_amount=data.get('gst_amount'), total_amount=str(data.get('total_amount')), remarks=data.get('remarks'), timestamp=datetime.utcnow())
        elif category == "FWL":
            new_entry = FWLTable(filename=filename, clinic_name=data.get('bill_to'), total_payable=str(data.get('total_amount')), remarks=data.get('remarks'), timestamp=datetime.utcnow())
        db.add(new_entry); db.commit(); db.refresh(new_entry); rid = new_entry.id; db.close(); return rid
    except Exception as e: log_to_ui(f"DB Error: {str(e)}", type="error"); return None

def update_google_sheet(amount, category, filename, record_id=None):
    try:
        mapping = {"CK": "C39", "SP": "C42", "FWL": "C68"}
        labels = {"CK": "CK Secreterial", "SP": "SP (Firmus Cap)", "FWL": "Foreign Worker Levy"}
        cell, label = mapping.get(category, "C39"), labels.get(category, "Item")
        creds = get_credentials()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID); worksheet = sh.get_worksheet(0) 
        old_val = worksheet.acell(cell).value
        worksheet.update_acell(cell, amount)
        db = SessionLocal()
        audit_entry = CellChange(sheet_name="Settlement Sheet", cell_reference=cell, label_name=f"{label} (via OCR Batch)" if "Batch" in filename else f"{label} (via OCR: {filename})", old_value=str(old_val), new_value=str(amount), source_table=category, source_id=record_id, timestamp=datetime.utcnow())
        db.add(audit_entry); db.commit(); db.close()
        log_to_ui(f"Updated {cell} with {amount}!", type="success")
    except Exception as e: log_to_ui(f"Sync Error: {str(e)}", type="error")

# --- EXTRACTION & OCR ---
def extract_invoice_data(text, filename=""):
    data = {'total_amount': "Not Found", 'sub_total': "Not Found", 'gst_amount': "0.00", 'remarks': ""}
    category = "CK"
    if re.search(r"(?i)FWL|Foreign\s+Worker\s+Levy", text) or re.search(r"(?i)FWL", filename):
        category = "FWL"; data['supplier_name'] = "MOM (FWL)"
    elif re.search(r"(?i)Firmus\s+Cap", text) or re.search(r"(?i)SP|Firmus", filename):
        category = "SP"; data['supplier_name'] = "Firmus Cap"
    elif re.search(r"(?i)CK\s+SECRETARIAL", text) or re.search(r"(?i)CK", filename):
        category = "CK"; data['supplier_name'] = "CK SECRETARIAL SERVICES PTE LTD"
    
    date_match = re.search(r"(?i)Date\s*[:\s]*([\d/]{6,})", text)
    data['invoice_date'] = date_match.group(1) if date_match else "Not Found"
    inv_no_match = re.search(r"(?i)(Invoice|Tax\s+Invoice)\s+No\.\s*[:\s]*([\d]+)", text)
    data['invoice_no'] = inv_no_match.group(2) if inv_no_match else "Not Found"
    bill_to_match = re.search(r"(?i)(INVOICE|Bill\s+To|Delivered\s+To|Clinic\s+Name)\s*[:\s]*([\w\s]+PTE\s+LTD)", text)
    if not bill_to_match: bill_to_match = re.search(r"(?i)(CASA\s+DENTAL\s+[\w\s]+PTE\s+LTD)", text)
    data['bill_to'] = bill_to_match.group(2).strip() if bill_to_match and len(bill_to_match.groups()) > 1 else (bill_to_match.group(1).strip() if bill_to_match else "Not Found")
    
    all_amounts = re.findall(r"[\d,]+\.\d{2}", text)
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    for i, line in enumerate(lines):
        if re.search(r"(?i)\b(Total|Grand\s*Total|Total\s*Payable)\b", line) and not re.search(r"(?i)sub", line):
            amt = re.search(r"\$?\s*([\d,]+\.\d{2})", line)
            if not amt and i+1 < len(lines): amt = re.search(r"^\$?\s*([\d,]+\.\d{2})", lines[i+1])
            if amt: data['total_amount'] = amt.group(1).replace(",", "")
    if data['total_amount'] == "Not Found" and all_amounts: data['total_amount'] = all_amounts[-1].replace(",", "")
    
    remarks_lines = []
    found_particulars = False
    for line in lines:
        if re.search(r"(?i)Particulars|Description|Details", line): found_particulars = True; continue
        if re.search(r"(?i)Sub\s*Total|Total|Payable", line): break
        if found_particulars and len(line) > 5: remarks_lines.append(line)
    data['remarks'] = " | ".join(remarks_lines) if remarks_lines else "Not Found"
    return data, category

def process_document(file_content, mime_type):
    try:
        credentials = get_credentials()
        processor_id = get_processor_id()
        client = documentai.DocumentProcessorServiceClient(client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}, credentials=credentials)
        # Re-derive project_id from credentials
        project_id = "casa-dental-ops"
        if "gcp_service_account" in st.secrets:
            project_id = st.secrets["gcp_service_account"]["project_id"]
        else:
            with open("credentials.json", "r") as f: project_id = json.load(f)["project_id"]

        name = client.processor_path(project_id, LOCATION, processor_id)
        raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)
        return client.process_document(request=request).document.text
    except Exception as e: log_to_ui(f"ERROR: {str(e)}", type="error"); return None

# --- STREAMLIT UI ---
st.set_page_config(page_title="Casa Dental Hub", page_icon="🦷", layout="wide")
st.title("🦷 Casa Dental - Operations Hub")

Base.metadata.create_all(bind=engine)
start_background_tracker()

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    st.markdown("### Step 1: Upload Files")
    uploaded_files = st.file_uploader("Drop invoices here", type=["pdf", "png", "jpg", "jpeg", "tiff"], accept_multiple_files=True)
    if uploaded_files:
        if st.button("🚀 Start OCR Processing", type="primary"):
            st.divider(); sp_batch = []; st.session_state['pending_fwl'] = []
            for uploaded_file in uploaded_files:
                with st.status(f"🔍 Analyzing: {uploaded_file.name}") as status:
                    file_bytes = uploaded_file.read()
                    extracted_text = process_document(file_bytes, uploaded_file.type)
                    if extracted_text:
                        inv_data, category = extract_invoice_data(extracted_text, uploaded_file.name)
                        if category == "SP":
                            save_to_db(uploaded_file.name, inv_data, "SP"); sp_batch.append(float(inv_data['total_amount']))
                        elif category == "FWL":
                            st.session_state['pending_fwl'].append({"filename": uploaded_file.name, "data": inv_data})
                        else:
                            rec_id = save_to_db(uploaded_file.name, inv_data, category)
                            update_google_sheet(inv_data['total_amount'], category, uploaded_file.name, rec_id)
                        status.update(label=f"Done: {uploaded_file.name}", state="complete")
            if sp_batch:
                st.session_state['sp_batch_total'] = sum(sp_batch); st.session_state['sp_batch_count'] = len(sp_batch)

        if st.session_state.get('pending_fwl'):
            st.divider(); st.subheader("🏥 Pending FWL Confirmation")
            for idx, item in enumerate(st.session_state['pending_fwl']):
                col_a, col_b, col_c = st.columns([2, 2, 1])
                with col_a: st.write(f"**File:** {item['filename']}")
                with col_b: clinic = st.selectbox(f"Clinic for {item['filename']}", CLINICS, key=f"sel_{idx}")
                with col_c:
                    if st.button(f"Sync: {item['filename']}", key=f"btn_{idx}"):
                        item['data']['bill_to'] = clinic; rid = save_to_db(item['filename'], item['data'], "FWL")
                        update_google_sheet(item['data']['total_amount'], "FWL", item['filename'], rid)
                        st.session_state['pending_fwl'].pop(idx); st.rerun()

        if 'sp_batch_total' in st.session_state:
            st.divider(); st.subheader("📊 SP Batch Ready")
            st.info(f"Total Sum: **${st.session_state['sp_batch_total']:.2f}**")
            if st.button("🚀 Sync SP Total to C42"):
                update_google_sheet(f"{st.session_state['sp_batch_total']:.2f}", "SP", "Batch Update")
                del st.session_state['sp_batch_total']; st.balloons()

with tab2:
    st.subheader("📊 Database Records")
    
    # CK Secreterial Table
    st.markdown("### 📑 CK Secreterial (C39)")
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
                "Remarks": r.remarks,
                "Time": r.timestamp.strftime("%Y-%m-%d %H:%M:%S")
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

    st.divider()
    st.markdown("### 🔍 Full Audit Trail")
    try:
        db = SessionLocal()
        results = db.query(CellChange).order_by(CellChange.timestamp.desc()).limit(100).all()
        db.close()
        if results:
            data = [{
                "Table": r.source_table if r.source_table else "Manual",
                "Record ID": r.source_id if r.source_id else "-",
                "Cell": r.cell_reference,
                "Label": r.label_name,
                "Old": r.old_value,
                "New": r.new_value,
                "Time": r.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            } for r in results]
            st.dataframe(pd.DataFrame(data), use_container_width=True)
    except Exception as e: st.error(str(e))
