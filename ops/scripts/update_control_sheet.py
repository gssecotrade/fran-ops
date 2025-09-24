#!/usr/bin/env python3
# ops/scripts/update_control_sheet.py
import os, json, time
from datetime import datetime, timezone

# deps ligeras (ya las usas para Sheets)
import gspread
from google.oauth2.service_account import Credentials

DIST_DIR = os.environ.get("DIST_DIR", "dist")
REPORT_JSON = os.path.join(DIST_DIR, "report.json")

SHEET_ID = os.environ["CONTROL_SHEET_ID"]          # ← lo pasaremos por Secrets/vars
TAB_NAME  = os.environ.get("CONTROL_TAB_NAME", "Control_Pipeline")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def load_sa():
    raw = os.environ.get("GOOGLE_SA_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SA_JSON no presente en el entorno (secret).")
    data = json.loads(raw)
    creds = Credentials.from_service_account_info(data, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def read_report():
    # report.json lo genera make_report.py en el job build
    if not os.path.exists(REPORT_JSON):
        return {}
    with open(REPORT_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def fmt(x):
    if x is None: return ""
    if isinstance(x, (list, dict)): return json.dumps(x, ensure_ascii=False)
    return str(x)

def main():
    # Timestamps
    now_utc = datetime.now(timezone.utc)
    ts_iso  = now_utc.strftime("%Y-%m-%d %H:%M:%S %Z")

    report = read_report()
    # Campos esperados del report
    status = (report.get("status") or {}).get("status", "UNKNOWN")
    warn   = (report.get("status") or {}).get("warn", 0)
    fail   = (report.get("status") or {}).get("fail", 0)
    manifest = report.get("manifest", "")
    master   = "sí" if report.get("master_csv_ok") else "no"
    zips     = ", ".join(report.get("zips", []))
    links    = ", ".join([f'{k}:{v}' for k,v in (report.get("drive_links") or {}).items()])
    panel_url = report.get("panel_url", "")

    # Abre hoja
    client = load_sa()
    sh = client.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_NAME, rows=100, cols=20)
        ws.append_row([
            "timestamp_utc","dag_run_id","csv_count","rows_total",
            "dq_status","warn","fail",
            "manifest_path","master_csv","zip_loterias","zip_legales","zip_marketing",
            "panel_url","drive_links"
        ])

    # Valores del report que ya calculas
    csv_count  = report.get("csv_count", "")
    rows_total = report.get("rows_total", "")
    dag_run_id = report.get("dag_run_id", "")

    # Desglosa nombres ZIP si están
    z_lot = z_leg = z_mkt = ""
    for z in report.get("zips", []):
        if z.startswith("loterias_"):  z_lot = z
        elif z.startswith("legales_"): z_leg = z
        elif z.startswith("marketing_"): z_mkt = z

    row = [
        ts_iso, dag_run_id, csv_count, rows_total,
        status, warn, fail,
        manifest, master, z_lot, z_leg, z_mkt,
        panel_url, links
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    print("✓ Hoja de Control actualizada:", TAB_NAME)

if __name__ == "__main__":
    main()
