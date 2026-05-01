import sqlite3
import time

import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = "1FLeADEkmIJTJ-8E88lELpiJX1ARoK5D4tjSn2qcsU10"
SETTLEMENT_GID = 305885354


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    gc = gspread.authorize(
        Credentials.from_service_account_file("credentials.json", scopes=scopes)
    )
    ss = gc.open_by_key(SHEET_ID)
    ws = ss.get_worksheet_by_id(SETTLEMENT_GID)

    original = ws.acell("C39").value or "0"
    orig = float(str(original).replace(",", ""))
    test_val = f"{orig + 0.11:.2f}"

    con = sqlite3.connect("settlement_tracking.db")
    cur = con.cursor()
    ck_before = cur.execute("select count(*) from ck_secreterial").fetchone()[0]
    aud_before = cur.execute(
        "select count(*) from cell_changes where cell_reference='C39'"
    ).fetchone()[0]
    con.close()

    print("ORIGINAL", f"{orig:.2f}")
    print("SETTING", test_val)
    ws.update_acell("C39", test_val)

    time.sleep(70)

    con = sqlite3.connect("settlement_tracking.db")
    cur = con.cursor()
    ck_after = cur.execute("select count(*) from ck_secreterial").fetchone()[0]
    aud_after = cur.execute(
        "select count(*) from cell_changes where cell_reference='C39'"
    ).fetchone()[0]
    last_ck = cur.execute(
        "select id, filename, total_amount, remarks, timestamp from ck_secreterial order by id desc limit 1"
    ).fetchone()
    last_aud = cur.execute(
        "select id, old_value, new_value, source_table, timestamp from cell_changes where cell_reference='C39' order by id desc limit 1"
    ).fetchone()
    con.close()

    print("CK_DELTA", ck_after - ck_before)
    print("AUDIT_DELTA", aud_after - aud_before)
    print("LAST_CK", last_ck)
    print("LAST_AUDIT", last_aud)

    ws.update_acell("C39", f"{orig:.2f}")
    print("RESTORED", f"{orig:.2f}")


if __name__ == "__main__":
    main()
