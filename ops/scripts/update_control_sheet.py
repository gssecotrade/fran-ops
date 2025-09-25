#!/usr/bin/env python3
# ops/scripts/update_control_sheet.py

import os, json, base64, datetime
import gspread
from google.oauth2 import service_account

# -------- Config de entorno --------
SHEET_ID  = os.environ["CONTROL_SHEET_ID"]
TAB_NAME  = os.environ.get("CONTROL_SHEET_TAB", "Control_Pipeline")
PANEL_URL = os.environ.get("PANEL_URL", "")
RUN_ID    = os.environ.get("GITHUB_RUN_ID", "")

DIST_DIR = os.environ.get("DIST_DIR", "dist")
REPORT_PATH = os.path.join(DIST_DIR, "report.json")

# -------- Helper: cargar SA desde secret --------
def load_gspread_client():
    # Preferimos GOOGLE_SA_JSON_BASE64; si no, GOOGLE_SA_JSON (texto JSON).
    b64 = os.environ.get("GOOGLE_SA_JSON_BASE64")
    raw = os.environ.get("GOOGLE_SA_JSON")

    if b64:
        info = json.loads(base64.b64decode(b64).decode("utf-8"))
    elif raw:
        info = json.loads(raw)
    else:
        raise RuntimeError("Falta GOOGLE_SA_JSON_BASE64 o GOOGLE_SA_JSON en secrets/env.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# -------- Helper: abrir/crear pestaña --------
def open_or_create_ws(gc, sheet_id: str, tab_name: str):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=200, cols=20)
    return ws

# -------- Aplanar valores a string --------
def flat(v):
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        if isinstance(v, bool):  # pasamos bool a "sí"/"no"
            return "sí" if v else "no"
        return str(v)
    # dict/list -> JSON compacta
    try:
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(v)

# -------- Cargar report.json si existe --------
def load_report(path: str):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # fallback mínimo para no romper
    return {
        "status": {"overall": "UNKNOWN", "warn": 0, "fail": 0},
        "manifest": "",
        "master_csv": False,
        "zips": [],
        "drive_links": [],
        "updated_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds"),
    }

# -------- Upsert cabecera --------
HEADER = [
    "timestamp_utc",
    "run_id",
    "status_overall",
    "warn",
    "fail",
    "manifest",
    "master_csv",
    "zip_loterias",
    "zip_marketing",
    "zip_legales",
    "panel_url",
]

def ensure_header(ws):
    try:
        first_row = ws.row_values(1)
    except gspread.exceptions.APIError:
        first_row = []
    if first_row != HEADER:
        if first_row:
            ws.delete_rows(1)
        ws.insert_row(HEADER, index=1)

# -------- Principal --------
def main():
    gc = load_gspread_client()
    ws = open_or_create_ws(gc, SHEET_ID, TAB_NAME)
    ensure_header(ws)

    report = load_report(REPORT_PATH)

    # Extraer info del report en tipos planos
    status = report.get("status", {}) or {}
    overall = status.get("overall", "UNKNOWN")
    warn = status.get("warn", 0)
    fail = status.get("fail", 0)

    manifest = report.get("manifest", "")
    master_csv = report.get("master_csv", False)

    # Identificar zips por nombre (si existen)
    zips = report.get("zips", []) or []
    def find_zip(prefix):
        for z in zips:
            if isinstance(z, dict):
                name = z.get("name", "")
            else:
                name = str(z)
            if name.startswith(prefix):
                return name
        return ""

    zip_loterias  = find_zip("loterias_")
    zip_marketing = find_zip("marketing_")
    zip_legales   = find_zip("legales_")

    # Construir fila (todo plano)
    now_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat()
    row = [
        flat(now_utc),
        flat(RUN_ID),
        flat(overall),
        flat(warn),
        flat(fail),
        flat(manifest),
        flat(master_csv),
        flat(zip_loterias),
        flat(zip_marketing),
        flat(zip_legales),
        flat(PANEL_URL),
    ]

    ws.append_row(row, value_input_option="USER_ENTERED")
    print("✅ Control_Pipeline actualizado.")

if __name__ == "__main__":
    main()
