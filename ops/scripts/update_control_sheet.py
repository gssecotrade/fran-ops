#!/usr/bin/env python3
# ops/scripts/update_control_sheet.py

import os, json, base64, sys
from datetime import datetime, timezone

# ---- GSpread / Google APIs ----
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = os.environ.get("CONTROL_SHEET_ID")  # obligatoria (GitHub Secret)
SHEET_TAB = os.environ.get("CONTROL_SHEET_TAB", "Control_Pipeline")
REPORT_PATHS = [
    "dist/report.json",                 # pipeline GH Pages
    os.path.join("ops", "dist", "report.json"),  # por si se ejecuta local distinto
]

def _now_utc_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def load_report():
    for p in REPORT_PATHS:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    # fallback mínimo si no hay report.json
    return {
        "updated_utc": _now_utc_iso(),
        "status": {"OK": "✅OK", "WARN": "⚠️WARN", "FAIL": "❌FAIL"},
        "dq_overall": "UNKNOWN",
        "manifest": None,
        "master_csv": False,
        "zips": [],
        "drive_links": [],
        "panel_url": os.environ.get("PANEL_URL", ""),
    }

def load_sa_credentials_dict():
    """
    Soporta:
      - GOOGLE_SA_JSON (JSON en claro)
      - GOOGLE_SA_JSON_BASE64 (tu secreto actual)
      - GOOGLE_SA_JSON_FILE (ruta a fichero)
    """
    raw = os.environ.get("GOOGLE_SA_JSON")
    if raw:
        return json.loads(raw)

    b64 = os.environ.get("GOOGLE_SA_JSON_BASE64")
    if b64:
        try:
            txt = base64.b64decode(b64).decode("utf-8")
            return json.loads(txt)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SA_JSON_BASE64 inválido: {e}")

    fpath = os.environ.get("GOOGLE_SA_JSON_FILE")
    if fpath and os.path.exists(fpath):
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError("No hay credenciales: defina GOOGLE_SA_JSON o GOOGLE_SA_JSON_BASE64 o GOOGLE_SA_JSON_FILE.")

def get_gspread_client():
    sa_dict = load_sa_credentials_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

def ensure_header(ws, header):
    """Asegura cabeceras en la fila 1 si está vacía."""
    first = ws.row_values(1)
    if not first:
        ws.update("A1", [header])

def to_row_from_report(report):
    status = report.get("dq_overall") or "UNKNOWN"
    status_json = json.dumps(report.get("status", {}), ensure_ascii=False)
    manifest = report.get("manifest") or ""
    master = "sí" if report.get("master_csv") else "no"
    zips = ", ".join(report.get("zips", []))
    links = ", ".join(report.get("drive_links", []))
    panel = report.get("panel_url", "")

    return [
        _now_utc_iso(),                      # timestamp_utc
        os.environ.get("GITHUB_RUN_ID", ""), # run_id
        status,                              # dq_status
        status_json,                         # dq_status_json
        manifest,                            # manifest_path
        master,                              # master_csv
        zips,                                # zip_files
        links,                               # drive_links
        panel,                               # panel_url
    ]

def main():
    if not SHEET_ID:
        raise RuntimeError("CONTROL_SHEET_ID no presente en el entorno (secret).")

    report = load_report()
    gc = get_gspread_client()

    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=20)

    header = [
        "timestamp_utc",
        "run_id",
        "dq_status",
        "dq_status_json",
        "manifest_path",
        "master_csv",
        "zip_files",
        "drive_links",
        "panel_url",
    ]
    ensure_header(ws, header)

    row = to_row_from_report(report)
    ws.append_row(row, value_input_option="USER_ENTERED")
    print(f"✓ Control sheet actualizado en pestaña '{SHEET_TAB}'. Fila añadida.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", repr(e))
        sys.exit(1)
