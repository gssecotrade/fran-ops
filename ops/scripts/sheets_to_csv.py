# -*- coding: utf-8 -*-
"""
Sheets -> CSV (Loterías)
Lee TODAS las hojas del Spreadsheet y genera un CSV por hoja en loterias/data.
Reglas:
- Se ignoran hojas cuyo nombre empiece por "_" (ocultas/descartes).
- Los nombres de fichero se normalizan (minúsculas, guiones).
Requiere:
- GOOGLE_SA_JSON: ruta al JSON del Service Account (lo crea el workflow)
- SHEETS_SPREADSHEET_ID: ID del Google Sheets
"""

import os, re, csv, sys
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT_DIR = os.path.join(BASE, "loterias", "data")

SA_PATH  = os.getenv("GOOGLE_SA_JSON", "")
SHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def sanitize(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\-]", "", s)
    return s or "sheet"

def die(msg):
    print(f"❌ {msg}")
    sys.exit(1)

def main():
    print("Sheets -> CSV (Loterías) · inicio")
    if not SA_PATH or not os.path.exists(SA_PATH):
        die("GOOGLE_SA_JSON no existe en el runner")
    if not SHEET_ID:
        die("SHEETS_SPREADSHEET_ID vacío")

    os.makedirs(OUT_DIR, exist_ok=True)

    creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    svc   = build("sheets", "v4", credentials=creds)

    # 1) Descubrir hojas
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID, includeGridData=False).execute()
    sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]
    print(f"→ Hojas detectadas: {len(sheets)}")

    total_csv = 0
    for title in sheets:
        if title.startswith("_"):
            print(f"   · salto hoja '{title}' (prefijo _)")  # hoja ignorada
            continue

        rng = f"{title}"
        values = svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute().get("values", [])

        # Si la hoja está vacía, crea CSV vacío con cabecera mínima
        out_name = f"{sanitize(title)}.csv"
        out_path = os.path.join(OUT_DIR, out_name)

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in values:
                writer.writerow(row)

        rows = len(values)
        print(f"   · {title} → {out_name} ({rows} filas)")
        total_csv += 1

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"✅ Generados {total_csv} CSV en {OUT_DIR} · {stamp}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(f"Error inesperado: {e}")
