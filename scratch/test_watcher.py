import gspread
import time
import sqlite3
from datetime import datetime

# Config
CREDENTIALS_PATH = "credentials.json"
SPREADSHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10"
TEST_VALUE = "777.77"

def test_watcher():
    print(f"--- Starting Watcher Test at {datetime.now()} ---")
    
    # 1. Authenticate and update Google Sheet
    gc = gspread.service_account(filename=CREDENTIALS_PATH)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)
    
    print(f"Changing cell C39 to {TEST_VALUE}...")
    worksheet.update_acell('C39', TEST_VALUE)
    
    # 2. Wait
    print(f"Waiting 15 seconds for poll...")
    time.sleep(15)
    
    # 3. Check SQLite
    conn = sqlite3.connect("settlement_tracking.db")
    cursor = conn.cursor()
    cursor.execute("SELECT filename, total_amount, timestamp FROM ck_secreterial ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    
    if row:
        filename, amount, timestamp = row
        print(f"Latest Entry: {filename} | {amount} | {timestamp}")
        if amount == TEST_VALUE:
            print("SUCCESS: Tracker detected the change!")
        else:
            print(f"FAIL: Expected {TEST_VALUE}, found {amount}")
    else:
        print("FAIL: No entries found.")

if __name__ == "__main__":
    test_watcher()
