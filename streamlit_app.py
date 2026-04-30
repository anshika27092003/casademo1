import streamlit as st
from google.cloud import documentai_v1 as documentai
from google.oauth2 import service_account
import json
import logging
import re
import pandas as pd
import gspread
from io import BytesIO
from datetime import datetime

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_to_ui(message, type="info"):
    logger.info(message)
    if type == "info": st.info(message)
    elif type == "success": st.success(message)
    elif type == "error": st.error(message)
    elif type == "warning": st.warning(message)

# --- CONFIGURATION (CLOUD READY) ---
LOCATION = "us" 
SPREADSHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10"

# Helper to get credentials from Secrets (Cloud) or File (Local)
def get_credentials():
    if "gcp_service_account" in st.secrets:
        return dict(st.secrets["gcp_service_account"])
    else:
        with open("credentials.json", "r") as f:
            return json.load(f)

# --- GOOGLE SHEETS "DATABASE" LOGIC ---
def get_gsheet_client():
    creds_data = get_credentials()
    return gspread.service_account_from_dict(creds_data)

def save_to_gsheet(data):
    """Saves extracted data to a 'OCR_RECORDS' tab in the Google Sheet."""
    try:
        gc = get_gsheet_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        # Try to find or create the OCR_RECORDS worksheet
        try:
            worksheet = sh.worksheet("OCR_RECORDS")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title="OCR_RECORDS", rows="100", cols="20")
            worksheet.append_row([
                "Timestamp", "Filename", "Supplier", "Reg No", "Date", 
                "Inv No", "Bill To", "Sub Total", "GST", "Total", "Remarks"
            ])
        
        row = [
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            data.get('filename'),
            data.get('supplier_name'),
            data.get('consignment_number'),
            data.get('invoice_date'),
            data.get('invoice_no'),
            data.get('bill_to'),
            data.get('sub_total'),
            data.get('gst_amount'),
            data.get('total_amount'),
            data.get('remarks')
        ]
        worksheet.append_row(row)
        log_to_ui(f"Saved entry to Google Sheet 'OCR_RECORDS' tab!", type="success")
    except Exception as e:
        log_to_ui(f"Failed to save to Google Sheet: {str(e)}", type="error")

def update_settlement_cell(amount):
    """Updates cell C39 in the main settlement tab."""
    try:
        gc = get_gsheet_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.get_worksheet(0) # Main tab
        worksheet.update_acell('C39', amount)
        log_to_ui(f"Updated Settlement Cell C39 with ${amount}!", type="success")
    except Exception as e:
        log_to_ui(f"Failed to update Cell C39: {str(e)}", type="error")

# --- EXTRACTION ENGINE ---
def extract_invoice_data(text):
    data = {'total_amount': "Not Found", 'sub_total': "Not Found", 'gst_amount': "0.00", 'remarks': ""}
    if re.search(r"(?i)CK\s+SECRETARIAL\s+SERVICES", text): data['supplier_name'] = "CK SECRETARIAL SERVICES PTE LTD"
    reg_match = re.search(r"(?i)Registration\s+Number\s*[:\s]*([\w]+)", text)
    data['consignment_number'] = reg_match.group(1) if reg_match else "Not Found"
    date_match = re.search(r"(?i)Date\s*[:\s]*([\d/]{6,})", text)
    data['invoice_date'] = date_match.group(1) if date_match else "Not Found"
    inv_no_match = re.search(r"(?i)Invoice\s+No\.\s*[:\s]*([\d]+)", text)
    data['invoice_no'] = inv_no_match.group(1) if inv_no_match else "Not Found"
    bill_to_match = re.search(r"(?i)INVOICE\s*\n\s*([\w\s]+PTE\s+LTD)", text)
    data['bill_to'] = bill_to_match.group(1).strip() if bill_to_match else "Not Found"
    
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    for i, line in enumerate(lines):
        if re.search(r"(?i)\bTotal\b", line) and not re.search(r"(?i)sub", line):
            amt = re.search(r"\$?\s*([\d,]+\.\d{2})", line)
            if not amt and i+1 < len(lines): amt = re.search(r"^\$?\s*([\d,]+\.\d{2})", lines[i+1])
            if amt: data['total_amount'] = amt.group(1).replace(",", "")
        if re.search(r"(?i)Sub\s+Total", line):
            amt = re.search(r"\$?\s*([\d,]+\.\d{2})", line)
            if amt: data['sub_total'] = amt.group(1).replace(",", "")
        if re.search(r"(?i)GST", line):
            amt = re.search(r"\$?\s*([\d,]+\.\d{2})", line)
            if amt: data['gst_amount'] = amt.group(1).replace(",", "")

    remarks_lines = []
    found_particulars = False
    for line in lines:
        if re.search(r"(?i)Particulars", line): found_particulars = True; continue
        if re.search(r"(?i)Sub\s*Total|Total", line): break
        if found_particulars and len(line) > 5: remarks_lines.append(line)
    data['remarks'] = " | ".join(remarks_lines) if remarks_lines else "Not Found"
    return data

# --- OCR ENGINE ---
def process_document(file_content, mime_type):
    try:
        creds_data = get_credentials()
        credentials = service_account.Credentials.from_service_account_info(creds_data)
        client = documentai.DocumentProcessorServiceClient(
            client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}, 
            credentials=credentials
        )
        name = client.processor_path(creds_data["project_id"], LOCATION, creds_data["processor_id"])
        raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)
        return client.process_document(request=request).document.text
    except Exception as e:
        log_to_ui(f"OCR ERROR: {str(e)}", type="error"); return None

# --- STREAMLIT UI ---
st.set_page_config(page_title="Casa Dental Hub", page_icon="🦷", layout="wide")
st.title("🦷 Casa Dental - Operations Hub")

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    uploaded_files = st.file_uploader("Choose files", type=["pdf", "png", "jpg", "jpeg", "tiff"], accept_multiple_files=True)
    if uploaded_files:
        st.divider()
        for uploaded_file in uploaded_files:
            with st.expander(f"📄 Processing: {uploaded_file.name}", expanded=True):
                file_bytes = uploaded_file.read()
                extracted_text = process_document(file_bytes, uploaded_file.type)
                if extracted_text:
                    inv_data = extract_invoice_data(extracted_text)
                    inv_data['filename'] = uploaded_file.name
                    if inv_data['total_amount'] != "Not Found":
                        save_to_gsheet(inv_data)
                        update_settlement_cell(inv_data['total_amount'])
                        st.success(f"Processed: Total ${inv_data['total_amount']}")
                        st.json(inv_data)
                    else: st.warning("No Total found.")
                else: st.error("OCR Failed.")

with tab2:
    st.subheader("📊 Google Sheets Records")
    try:
        gc = get_gsheet_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet("OCR_RECORDS")
        data = worksheet.get_all_records()
        if data:
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else: st.info("No records in 'OCR_RECORDS' tab yet.")
    except Exception as e:
        st.error(f"Connect to Google Sheet to see records: {str(e)}")
