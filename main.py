from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from datetime import datetime
import os
import threading
import time
import logging
import csv
import requests
from io import StringIO
import re
from contextlib import asynccontextmanager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheet Config
# We use the export?format=csv URL to easily download the public sheet data
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10/export?format=csv&gid=305885354"

from database import SessionLocal, Base, engine, CellChange, CKSecreterial

# Create tables (ensures they exist)
Base.metadata.create_all(bind=engine)

# Pydantic Schemas for API
from typing import Optional

class ChangeResponse(BaseModel):
    id: int
    sheet_name: str
    cell_reference: str
    label_name: str
    old_value: Optional[str] = None
    new_value: str
    timestamp: datetime

    class Config:
        from_attributes = True

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- GOOGLE SHEETS POLLER LOGIC ---

def get_column_letter(n):
    """Convert 1-indexed column number to Excel letter (e.g. 1 -> A, 2 -> B)"""
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def load_google_sheet_state(url):
    """Downloads the CSV export of the Google Sheet and extracts its data values."""
    logger.debug(f"Fetching Google Sheet...")
    state = {}
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            sheet_name = "AMK SETTLEMENT" # Hardcoded based on the gid provided
            sheet_data = {}
            
            f = StringIO(response.text)
            reader = csv.reader(f)
            
            for row_idx, row in enumerate(reader):
                for col_idx, value in enumerate(row):
                    if value.strip() != "":
                        cell_ref = f"{get_column_letter(col_idx + 1)}{row_idx + 1}"
                        sheet_data[cell_ref] = value.strip()
                        
            state[sheet_name] = sheet_data
        elif response.status_code in [401, 403]:
            logger.error("Access Denied! The Google Sheet is Private. Please change sharing settings to 'Anyone with the link can view'.")
        else:
            logger.error(f"Failed to fetch Google Sheet: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Error loading Google Sheet: {e}")
    return state

class GoogleSheetPoller:
    def __init__(self, url, db_session_maker):
        self.url = url
        self.db_session_maker = db_session_maker
        self.last_state = load_google_sheet_state(self.url)
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.poll, daemon=True)
        self.thread.start()
        logger.info("Google Sheet background poller started. Watching for changes every 10 seconds...")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Google Sheet background poller stopped.")

    def poll(self):
        while self.running:
            time.sleep(10) # Poll every 10 seconds
            self.process_changes()

    def process_changes(self):
        new_state = load_google_sheet_state(self.url)
        if not new_state:
            return

        db = self.db_session_maker()
        changes_found = 0

        try:
            for sheet_name, new_sheet_data in new_state.items():
                old_sheet_data = self.last_state.get(sheet_name, {})
                
                # Check for changes in currently populated cells
                for cell_ref, new_val in new_sheet_data.items():
                    old_val = old_sheet_data.get(cell_ref)
                    if new_val != old_val:
                        # Find the label (assume Column A of the same row)
                        row_num = re.findall(r'\d+', cell_ref)[0]
                        label_ref = f"A{row_num}"
                        label_val = new_sheet_data.get(label_ref, "Unknown")
                        
                        # Save to database (Audit Trail)
                        db_change = CellChange(
                            sheet_name=sheet_name,
                            cell_reference=cell_ref,
                            label_name=str(label_val),
                            old_value=str(old_val) if old_val is not None else None,
                            new_value=str(new_val),
                            timestamp=datetime.utcnow()
                        )
                        db.add(db_change)
                        changes_found += 1

                        # SPECIAL CASE: If cell C39 is edited, also record it in ck_secreterial
                        if cell_ref == "C39":
                            ck_entry = CKSecreterial(
                                filename="Manual Entry",
                                supplier_name="CK SECRETARIAL SERVICES PTE LTD",
                                total_amount=str(new_val),
                                remarks=f"Manual edit in Google Sheet cell {cell_ref}",
                                timestamp=datetime.utcnow()
                            )
                            db.add(ck_entry)
                            logger.info(f"Detected manual edit to C39. Logged to ck_secreterial table.")
            
            if changes_found > 0:
                db.commit()
                logger.info(f"Recorded {changes_found} cell changes from Google Sheets!")
                
            self.last_state = new_state
        except Exception as e:
            logger.error(f"Error processing changes: {e}")
            db.rollback()
        finally:
            db.close()


# Background Thread Control
poller = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global poller
    poller = GoogleSheetPoller(GOOGLE_SHEET_URL, SessionLocal)
    poller.start()
    
    yield
    
    # Shutdown
    if poller:
        poller.stop()


app = FastAPI(title="Google Sheets Tracking API", lifespan=lifespan)

@app.get("/api/changes", response_model=list[ChangeResponse])
def get_changes(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Retrieve tracked changes from the database."""
    changes = db.query(CellChange).order_by(CellChange.timestamp.desc()).offset(skip).limit(limit).all()
    return changes

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
