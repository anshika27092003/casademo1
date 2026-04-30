import requests
import csv
import time
import logging
import threading
from datetime import datetime
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI

# We use the export?format=csv URL to easily download the public sheet data
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10/export?format=csv&gid=305885354"

from database import SessionLocal, Base, engine, CellChange, CKSecreterial, SPTable, FWLTable

# Create tables (ensures they exist)
Base.metadata.create_all(bind=engine)

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_column_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def load_google_sheet_state(url):
    state = {}
    try:
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content.decode('utf-8')
            csv_reader = csv.reader(content.splitlines())
            rows = list(csv_reader)
            sheet_name = "Settlement Sheet"
            sheet_data = {}
            for r_idx, row in enumerate(rows):
                for c_idx, val in enumerate(row):
                    if val.strip():
                        cell_ref = f"{get_column_letter(c_idx+1)}{r_idx+1}"
                        sheet_data[cell_ref] = val.strip()
            state[sheet_name] = sheet_data
        return state
    except Exception as e:
        logger.error(f"Error loading Google Sheet: {e}")
    return state

class GoogleSheetPoller:
    def __init__(self, url, db_session_maker):
        self.url = url
        self.db_session_maker = db_session_maker
        self.last_state = load_google_sheet_state(self.url)
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self.poll, daemon=True).start()

    def poll(self):
        while self.running:
            time.sleep(10)
            self.process_changes()

    def process_changes(self):
        new_state = load_google_sheet_state(self.url)
        if not new_state: return
        db = self.db_session_maker()
        
        for sheet_name, new_sheet_data in new_state.items():
            old_sheet_data = self.last_state.get(sheet_name, {})
            for cell_ref, new_val in new_sheet_data.items():
                old_val = old_sheet_data.get(cell_ref)
                if new_val != old_val:
                    row_num = re.findall(r'\d+', cell_ref)[0]
                    label_ref = f"A{row_num}"
                    label_val = new_sheet_data.get(label_ref, "Unknown")
                    
                    source_table = None
                    source_id = None
                    
                    # Log Manual Entries to specific tables
                    if cell_ref == "C39":
                        source_table = "CK"
                        entry = CKSecreterial(filename="Manual Entry", supplier_name="CK SECRETARIAL SERVICES PTE LTD", total_amount=str(new_val), remarks="Manual edit in Google Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_id = entry.id
                        logger.info(f"Logged manual C39 to CK table (ID: {source_id})")
                    elif cell_ref == "C42":
                        source_table = "SP"
                        entry = SPTable(filename="Manual Entry", supplier_name="Firmus Cap", total_amount=str(new_val), remarks="Manual edit in Google Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_id = entry.id
                        logger.info(f"Logged manual C42 to SP table (ID: {source_id})")
                    elif cell_ref == "C68":
                        source_table = "FWL"
                        entry = FWLTable(filename="Manual Entry", total_payable=str(new_val), remarks="Manual edit in Google Sheet", timestamp=datetime.utcnow())
                        db.add(entry); db.flush(); source_id = entry.id
                        logger.info(f"Logged manual C68 to FWL table (ID: {source_id})")
                    
                    # Create linked audit entry
                    audit = CellChange(
                        sheet_name=sheet_name, cell_reference=cell_ref, label_name=str(label_val),
                        old_value=str(old_val), new_value=str(new_val),
                        source_table=source_table, source_id=source_id,
                        timestamp=datetime.utcnow()
                    )
                    db.add(audit)
                    
            self.last_state = new_state
        db.commit()
        db.close()

app = FastAPI()

@app.on_event("startup")
def startup_event():
    poller = GoogleSheetPoller(GOOGLE_SHEET_URL, SessionLocal)
    poller.start()
    logger.info("Google Sheet background poller started.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
