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
SP_EXTRACTED_FIELDS = [
    "supplier_name",
    "clinic_name",
    "invoice_date",
    "tax_invoice_number",
    "sub_total",
    "gst_9_percent",
    "total_amount",
    "remarks",
]

# FWL: user picks clinic; amount appends to C67 on that clinic's settlement worksheet.
FWL_CLINICS = [
    "AMK",
    "HOLLAND",
    "WOODLANDS / WDLS",
    "BUKIT BATOK / BB",
    "BEDOK",
    "ADMIRALTY / ADM",
    "TENGAH",
    "BOON KENG / BK",
]
# Map dropdown label -> substring that appears in the Google Sheet tab title (upper match).
FWL_CLINIC_WORKSHEET_KEY = {
    "AMK": "AMK",
    "HOLLAND": "HOLLAND",
    "WOODLANDS / WDLS": "WDLS",
    "BUKIT BATOK / BB": "BB",
    "BEDOK": "BEDOK",
    "ADMIRALTY / ADM": "ADM",
    "TENGAH": "TENGAH",
    "BOON KENG / BK": "BOON KENG",
}
FWL_SETTLEMENT_CELL = "C67"
SHEET_POLL_INTERVAL_SECONDS = 5
FOREGROUND_SYNC_COOLDOWN_SECONDS = 5

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


def fwl_sheet_state_key(worksheet_title):
    return f"FWL|{worksheet_title}|{FWL_SETTLEMENT_CELL}"


def resolve_fwl_worksheet(spreadsheet, clinic_label):
    """Return the settlement worksheet for the selected clinic (tab title contains keyword)."""
    keyword = FWL_CLINIC_WORKSHEET_KEY.get(clinic_label, clinic_label).upper()
    for ws in spreadsheet.worksheets():
        title_u = (ws.title or "").upper()
        if keyword in title_u and "SETTLEMENT" in title_u:
            return ws
    for ws in spreadsheet.worksheets():
        if keyword in (ws.title or "").upper():
            return ws
    return None


def update_fwl_sheet_for_clinic(clinic_label, amount_to_append, filename, record_id=None):
    """Append FWL total_payable to C67 on the clinic-specific settlement tab."""
    try:
        client = get_gsheet_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        settlement_ws = resolve_fwl_worksheet(spreadsheet, clinic_label)
        if not settlement_ws:
            log_to_ui(f"❌ No settlement worksheet found for clinic: {clinic_label}", type="error")
            return
        cell_ref = FWL_SETTLEMENT_CELL
        current_val = settlement_ws.acell(cell_ref).value
        final_amount = format_amount(parse_amount(current_val) + parse_amount(amount_to_append))
        settlement_ws.update_acell(cell_ref, final_amount)

        state_key = fwl_sheet_state_key(settlement_ws.title)
        db = SessionLocal()
        state = db.query(SheetState).filter(SheetState.cell_reference == state_key).first()
        if not state:
            state = SheetState(cell_reference=state_key, last_value=str(final_amount), last_updated=datetime.utcnow())
            db.add(state)
        else:
            state.last_value = str(final_amount)
            state.last_updated = datetime.utcnow()
        row_num = re.findall(r"\d+", cell_ref)[0]
        label_val = settlement_ws.acell(f"A{row_num}").value
        audit = CellChange(
            sheet_name=settlement_ws.title,
            cell_reference=state_key,
            label_name=str(label_val),
            old_value="FWL Upload",
            new_value=str(final_amount),
            source_table="FWL",
            source_id=record_id,
            timestamp=datetime.utcnow(),
        )
        db.add(audit)
        db.commit()
        db.close()
        log_to_ui(f"✅ Synced FWL for {clinic_label} → {settlement_ws.title}!{cell_ref} (${final_amount})", type="success")
    except Exception as e:
        if is_read_quota_error(e):
            logger.warning(f"FWL read quota hit (hidden from UI): {e}")
            return
        log_to_ui(f"❌ FWL Sheet Sync Error: {e}", type="error")

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

def normalize_sp_payload(data):
    """Keep SP extraction aligned to strict user-required keys."""
    payload = {
        "supplier_name": "Not Found",
        "clinic_name": "Not Found",
        "invoice_date": "Not Found",
        "tax_invoice_number": "Not Found",
        "sub_total": "Not Found",
        "gst_9_percent": "0.00",
        "total_amount": "Not Found",
        "remarks": "Not Found",
    }
    if isinstance(data, dict):
        mapping = {
            "supplier_name": data.get("supplier_name"),
            "clinic_name": data.get("clinic_name") or data.get("bill_to"),
            "invoice_date": data.get("invoice_date"),
            "tax_invoice_number": data.get("tax_invoice_number") or data.get("invoice_no"),
            "sub_total": data.get("sub_total"),
            "gst_9_percent": data.get("gst_9_percent") or data.get("gst_amount"),
            "total_amount": data.get("total_amount"),
            "remarks": data.get("remarks"),
        }
        for key, value in mapping.items():
            if value not in (None, ""):
                payload[key] = str(value)
    return payload

def normalize_fwl_payload(data, clinic_name=None):
    """Ensure FWL payload is DB-safe and consistent."""
    payload = {
        "clinic_name": clinic_name or "Not Found",
        "total_amount": "0.00",
        "remarks": "Not Found",
    }
    if isinstance(data, dict):
        if data.get("clinic_name") or data.get("bill_to"):
            payload["clinic_name"] = str(data.get("clinic_name") or data.get("bill_to"))
        if clinic_name:
            payload["clinic_name"] = str(clinic_name)
        raw_total = data.get("total_amount") or data.get("total_payable")
        if raw_total not in (None, ""):
            payload["total_amount"] = str(raw_total)
        if data.get("remarks") not in (None, ""):
            payload["remarks"] = str(data.get("remarks"))
    return payload

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

def is_read_quota_error(err: Exception) -> bool:
    msg = str(err).lower()
    return (
        "quota exceeded" in msg
        or "read requests per minute per user" in msg
        or "apierror: [429]" in msg
        or "429" in msg
    )

def call_with_quota_retry(fn, max_attempts=3, base_sleep=0.8):
    """Retry quota-throttled Google Sheet calls with small backoff."""
    last_err = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if not is_read_quota_error(e):
                raise
            time.sleep(base_sleep * (attempt + 1))
    if last_err:
        raise last_err


def get_cell_values_map(worksheet, cells):
    """Fetch multiple cells with one API call when possible."""
    try:
        values = worksheet.batch_get(cells)
        cell_map = {}
        for idx, cell_ref in enumerate(cells):
            raw = values[idx] if idx < len(values) else []
            if raw and isinstance(raw, list):
                first_row = raw[0] if len(raw) > 0 else []
                if isinstance(first_row, list):
                    val = first_row[0] if first_row else "0"
                else:
                    val = first_row
            else:
                val = "0"
            cell_map[cell_ref] = format_amount(parse_amount(val))
        return cell_map
    except Exception:
        # Fallback for API differences/older gspread behavior.
        return {
            c: format_amount(parse_amount(worksheet.acell(c).value or "0"))
            for c in cells
        }

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
        
        cell_map = {"CK": "C39", "SP": "C42"}
        cell_ref = cell_map.get(category)
        
        if cell_ref:
            db = SessionLocal()
            state = db.query(SheetState).filter(SheetState.cell_reference == cell_ref).first()
            final_amount = amount
            if category == "CK":
                # Prefer DB-tracked state for append baseline to avoid read quota failures.
                baseline_val = state.last_value if state else "0"
                if parse_amount(baseline_val) == 0:
                    try:
                        baseline_val = call_with_quota_retry(lambda: settlement.acell(cell_ref).value or "0")
                    except Exception:
                        baseline_val = "0"
                final_amount = format_amount(parse_amount(baseline_val) + parse_amount(amount))
            
            # Perform update
            call_with_quota_retry(lambda: settlement.update_acell(cell_ref, final_amount))
            
            # Audit log
            if not state:
                state = SheetState(cell_reference=cell_ref, last_value=str(final_amount), last_updated=datetime.utcnow())
                db.add(state)
            else:
                state.last_value = str(final_amount)
                state.last_updated = datetime.utcnow()

            try:
                label_val = call_with_quota_retry(lambda: settlement.acell(f"A{cell_ref[1:]}").value or "")
            except Exception:
                label_val = ""
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
            sp_data = normalize_sp_payload(data)
            entry = SPTable(
                filename=filename,
                supplier_name=sp_data.get('supplier_name'),
                clinic_name=sp_data.get('clinic_name'),
                invoice_date=sp_data.get('invoice_date'),
                tax_invoice_number=sp_data.get('tax_invoice_number'),
                sub_total=sp_data.get('sub_total'),
                gst_amount=sp_data.get('gst_9_percent'),
                total_amount=sp_data.get('total_amount'),
                remarks=sp_data.get('remarks'),
                timestamp=datetime.utcnow()
            )
        elif category == "FWL":
            fwl_data = normalize_fwl_payload(data)
            entry = FWLTable(
                filename=filename,
                clinic_name=fwl_data.get("clinic_name"),
                total_payable=fwl_data.get("total_amount"),
                remarks=fwl_data.get("remarks"),
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
        time.sleep(SHEET_POLL_INTERVAL_SECONDS)

def sync_sheet_changes_once():
    # AMK settlement sheet manual listener (CK/SP/FWL).
    cells = ["C39", "C42", "C67"]
    db = None
    try:
        client = get_gsheet_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        settlement = spreadsheet.get_worksheet_by_id(SETTLEMENT_GID)
        db = SessionLocal()

        current_values = get_cell_values_map(settlement, cells)
        for cell_ref in cells:
            current_val = current_values[cell_ref]
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
                elif cell_ref == "C67":
                    entry = FWLTable(
                        filename="Manual Entry",
                        clinic_name="AMK",
                        total_payable=normalized_current_val,
                        remarks="Manual edit in Sheet (C67)",
                        timestamp=datetime.utcnow(),
                    )
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

    return "5ec65c9f9a56298"

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
    found_description_start = False
    for line in lines:
        # Detect the start of the items table
        if re.search(r"(?i)Particulars|Description|Details|Item|Service", line):
            found_description_start = True
            continue
        
        # Detect the end of the items table
        if re.search(r"(?i)Sub\s*Total|Total|Amount\s*Due|Payable|Thank\s+You|Payment\s+Details", line):
            if found_description_start:
                break
        
        if found_description_start:
            # Clean up the line (remove quantities/prices if they are on the same line)
            # Typically items are on the left, amounts on the right. 
            # We just want the text part.
            clean_line = re.sub(r"[\d,]+\.\d{2}\s*$", "", line).strip()
            if len(clean_line) > 3:
                remarks_lines.append(clean_line)
    
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
        
        # Specific remark for Firmus Cap (often looks like "Utilities for ...")
        m_rem = re.search(r"(?i)(Utilities\s+for\s+[\d\-\/ ]+)", text)
        if m_rem: data['remarks'] = m_rem.group(1).strip()
        elif not remarks_lines:
            # Fallback for Firmus: look for lines between "S/N" and "Sub Total"
            f_rem = []
            f_start = False
            for line in lines:
                if re.search(r"S/N", line): f_start = True; continue
                if re.search(r"Sub\s+Total", line): break
                if f_start and len(line) > 5: f_rem.append(line)
            if f_rem: data['remarks'] = " | ".join(f_rem)

    # Keep SP remarks strictly as extracted description/service items.
    if category == "SP" and remarks_lines:
        data["remarks"] = " | ".join(remarks_lines)

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
# Foreground sync is throttled to avoid hitting Sheets read quotas.
now_ts = time.time()
last_sync_ts = st.session_state.get("last_foreground_sheet_sync_ts", 0.0)
if now_ts - last_sync_ts >= FOREGROUND_SYNC_COOLDOWN_SECONDS:
    sync_sheet_changes_once()
    st.session_state["last_foreground_sheet_sync_ts"] = now_ts
start_background_tracker()

tab1, tab2 = st.tabs(["📤 Upload Documents (OCR)", "📋 View Records"])

with tab1:
    st.subheader("🚀 Upload Invoices")
    uploaded_files = st.file_uploader("Drop invoice images or PDFs", type=['png', 'jpg', 'jpeg', 'pdf'], accept_multiple_files=True)
    
    if uploaded_files:
        if 'ocr_preview' not in st.session_state:
            st.session_state['ocr_preview'] = {}
        if 'auto_processed_ck' not in st.session_state:
            st.session_state['auto_processed_ck'] = set()
        if 'processed_sp_batch' not in st.session_state:
            st.session_state['processed_sp_batch'] = set()
        if 'processed_fwl_batch' not in st.session_state:
            st.session_state['processed_fwl_batch'] = set()

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
                        elif cat == "SP":
                            inv = normalize_sp_payload(inv)
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
        st.session_state['processed_sp_batch'] = {
            k for k in st.session_state['processed_sp_batch'] if k in current_keys
        }
        st.session_state['processed_fwl_batch'] = {
            k for k in st.session_state['processed_fwl_batch'] if k in current_keys
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

        sp_candidates = [
            (k, v) for k, v in st.session_state.get('ocr_preview', {}).items()
            if v.get("category") == "SP" and v.get("data") and k not in st.session_state['processed_sp_batch']
        ]
        if sp_candidates:
            st.info(f"SP ready for batch submit: {len(sp_candidates)} file(s)")
            if st.button("✅ Submit SP Batch"):
                sp_batch_total = 0.0
                submitted_count = 0
                for file_key, preview in sp_candidates:
                    sp_data = normalize_sp_payload(preview["data"])
                    rid = save_to_db(preview["filename"], sp_data, "SP")
                    if rid:
                        submitted_count += 1
                        sp_batch_total += parse_amount(sp_data.get("total_amount"))
                        st.session_state['processed_sp_batch'].add(file_key)
                if submitted_count > 0:
                    update_google_sheet(format_amount(sp_batch_total), "SP", "SP Batch Submit")
                    st.success(f"Submitted {submitted_count} SP record(s). Synced ${format_amount(sp_batch_total)} to settlement.")
                else:
                    st.error("No SP records were submitted. Please check extracted values.")

        fwl_candidates = [
            (k, v) for k, v in st.session_state.get('ocr_preview', {}).items()
            if v.get("category") == "FWL" and v.get("data") and k not in st.session_state['processed_fwl_batch']
        ]
        if fwl_candidates:
            st.info(f"FWL ready for batch submit: {len(fwl_candidates)} file(s). Select clinic and confirm.")
            fwl_clinic = st.selectbox("Clinic for FWL upload(s)", FWL_CLINICS, key="fwl_batch_clinic")
            fwl_confirm = st.checkbox(
                f"I confirm uploading these FWL document(s) for clinic: **{fwl_clinic}**",
                key="fwl_batch_confirm",
            )
            if st.button("✅ Submit FWL Batch", disabled=not fwl_confirm):
                fwl_batch_total = 0.0
                submitted_count = 0
                last_rid = None
                failed_files = []
                for file_key, preview in fwl_candidates:
                    inv = normalize_fwl_payload(preview["data"], clinic_name=fwl_clinic)
                    rid = save_to_db(preview["filename"], inv, "FWL")
                    if rid:
                        submitted_count += 1
                        fwl_batch_total += parse_amount(inv.get("total_amount"))
                        st.session_state['processed_fwl_batch'].add(file_key)
                        last_rid = rid
                    else:
                        failed_files.append(preview["filename"])
                if submitted_count > 0:
                    update_fwl_sheet_for_clinic(
                        fwl_clinic,
                        format_amount(fwl_batch_total),
                        "FWL Batch Submit",
                        last_rid,
                    )
                    st.success(
                        f"Submitted {submitted_count} FWL record(s) for {fwl_clinic}. "
                        f"Appended ${format_amount(fwl_batch_total)} to {FWL_SETTLEMENT_CELL} on that clinic's sheet."
                    )
                    if failed_files:
                        st.warning(
                            "Some FWL files failed to save: " + ", ".join(failed_files[:3]) +
                            ("..." if len(failed_files) > 3 else "")
                        )
                else:
                    if failed_files:
                        st.error(
                            "No FWL records were submitted. Save failed for: " +
                            ", ".join(failed_files[:3]) +
                            ("..." if len(failed_files) > 3 else "")
                        )
                    else:
                        st.error("No FWL records were submitted. Please check extracted values.")

        if st.button("✨ Process All"):
            for _, preview in st.session_state.get('ocr_preview', {}).items():
                if not preview.get("data"):
                    continue

                f_name = preview["filename"]
                inv = preview["data"]
                cat = preview["category"]

                with st.status(f"Processing {f_name}"):
                    if cat in ("SP", "FWL", "CK"):
                        continue
                    rid = save_to_db(f_name, inv, cat)
                    update_google_sheet(inv['total_amount'], cat, f_name, rid)

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
    SP_VIEW_COLUMNS = [
        "supplier_name",
        "clinic_name",
        "invoice_date",
        "tax_invoice_number",
        "sub_total",
        "gst_9_percent",
        "total_amount",
        "remarks",
        "timestamp",
    ]
    FWL_VIEW_COLUMNS = [
        "filename",
        "clinic_name",
        "total_payable",
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

    def map_sp_row(r):
        return {
            "supplier_name": r.supplier_name or "Not Found",
            "clinic_name": r.clinic_name or "Not Found",
            "invoice_date": r.invoice_date or "Not Found",
            "tax_invoice_number": r.tax_invoice_number or "Not Found",
            "sub_total": r.sub_total or "Not Found",
            "gst_9_percent": r.gst_amount or "0.00",
            "total_amount": r.total_amount or "Not Found",
            "remarks": r.remarks or "Not Found",
            "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if r.timestamp else "",
        }

    def map_fwl_row(r):
        return {
            "filename": r.filename or "Not Found",
            "clinic_name": r.clinic_name or "Not Found",
            "total_payable": r.total_payable or "Not Found",
            "remarks": r.remarks or "Not Found",
            "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if r.timestamp else "",
        }

    st.write("### CK")
    db = SessionLocal()
    ck_rows = db.query(CKSecreterial).order_by(CKSecreterial.timestamp.desc()).all()
    db.close()
    if ck_rows:
        ck_df = pd.DataFrame([map_ck_row(r) for r in ck_rows], columns=CK_VIEW_COLUMNS)
        st.dataframe(ck_df, use_container_width=True)

    st.write("### SP")
    db = SessionLocal()
    sp_rows = db.query(SPTable).order_by(SPTable.timestamp.desc()).all()
    db.close()
    if sp_rows:
        sp_df = pd.DataFrame([map_sp_row(r) for r in sp_rows], columns=SP_VIEW_COLUMNS)
        st.dataframe(sp_df, use_container_width=True)

    st.write("### FWL")
    db = SessionLocal()
    fwl_rows = db.query(FWLTable).order_by(FWLTable.timestamp.desc()).all()
    db.close()
    if fwl_rows:
        fwl_df = pd.DataFrame([map_fwl_row(r) for r in fwl_rows], columns=FWL_VIEW_COLUMNS)
        st.dataframe(fwl_df, use_container_width=True)
    
    st.write("### 🔍 System Logs")
    db = SessionLocal()
    logs = db.query(CellChange).order_by(CellChange.timestamp.desc()).all()
    db.close()
    if logs:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "Category": l.source_table or "Unknown",
                        "Cell": l.cell_reference,
                        "Old": l.old_value,
                        "New": l.new_value,
                        "Timestamp": l.timestamp.strftime("%Y-%m-%d %H:%M:%S") if l.timestamp else "",
                    }
                    for l in logs
                ]
            ),
            use_container_width=True,
        )
