import gspread
import sqlite3
import time
from datetime import datetime

CREDENTIALS_PATH = "credentials.json"
SPREADSHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10"

def test_direct_inputs():
    print("\n--- Testing Direct Manual Inputs (Sheet -> DB) ---")
    gc = gspread.service_account(filename=CREDENTIALS_PATH)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.get_worksheet(0)
    
    test_data = {
        "C39": "1111.11", # CK
        "C42": "2222.22", # SP
        "C68": "3333.33"  # FWL
    }
    
    for cell, val in test_data.items():
        print(f"Updating {cell} to {val} manually...")
        ws.update_acell(cell, val)
    
    print("Waiting 15 seconds for background tracker to sync...")
    time.sleep(15)
    
    conn = sqlite3.connect('settlement_tracking.db')
    cursor = conn.cursor()
    
    # Check CK
    cursor.execute("SELECT total_amount FROM ck_secreterial WHERE filename='Manual Entry' ORDER BY id DESC LIMIT 1")
    ck = cursor.fetchone()
    print(f"CK Table Check: {ck[0] if ck else 'FAILED'}")
    
    # Check SP
    cursor.execute("SELECT total_amount FROM sp_table WHERE filename='Manual Entry' ORDER BY id DESC LIMIT 1")
    sp = cursor.fetchone()
    print(f"SP Table Check: {sp[0] if sp else 'FAILED'}")
    
    # Check FWL
    cursor.execute("SELECT total_payable FROM fwl_table WHERE filename='Manual Entry' ORDER BY id DESC LIMIT 1")
    fwl = cursor.fetchone()
    print(f"FWL Table Check: {fwl[0] if fwl else 'FAILED'}")
    
    conn.close()

if __name__ == "__main__":
    test_direct_inputs()
