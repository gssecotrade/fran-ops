# -*- coding: utf-8 -*-
"""
Upload to Google Drive (REAL)
- Sube ZIPs de ./dist y CSVs de loterias/data al folder GDRIVE_FOLDER_ID
- Genera dist/drive_links.txt con "nombre | webViewLink"
Requiere:
  GOOGLE_SA_JSON (ruta al JSON del Service Account)
  GDRIVE_FOLDER_ID (ID de la carpeta en Drive compartida con el SA)
"""

import os, glob, mimetypes, sys
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SA_PATH = os.getenv("GOOGLE_SA_JSON", "")
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")

DIST_DIR = os.path.join(BASE, "dist")
LOT_DIR  = os.path.join(BASE, "loterias", "data")
LINKS_TXT = os.path.join(DIST_DIR, "drive_links.txt")

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def die(msg):
    print(f"❌ {msg}")
    sys.exit(1)

def mime_for(path):
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def client():
    if not SA_PATH or not os.path.exists(SA_PATH):
        die("GOOGLE_SA_JSON no existe en el runner")
    if not FOLDER_ID:
        die("GDRIVE_FOLDER_ID vacío")
    creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def upload_file(drv, path):
    name = os.path.basename(path)
    media = MediaFileUpload(path, mimetype=mime_for(path), resumable=False)
    body = {"name": name, "parents": [FOLDER_ID]}
    file = drv.files().create(body=body, media_body=media,
                              fields="id,name,webViewLink,parents").execute()
    return {"name": file["name"], "id": file["id"], "link": file.get("webViewLink", "")}

def main():
    os.makedirs(DIST_DIR, exist_ok=True)
    drv = client()

    targets = []
    targets += sorted(glob.glob(os.path.join(DIST_DIR, "*.zip")))
    targets += sorted(glob.glob(os.path.join(LOT_DIR, "*.csv")))

    if not targets:
        print("ℹ️ Nada que subir (no hay ZIP/CSV).")
        return

    uploads = []
    print(f"→ Subiendo {len(targets)} ficheros a Drive folder {FOLDER_ID}…")
    for p in targets:
        try:
            info = upload_file(drv, p)
            uploads.append(info)
            print(f"   · {info['name']} → {info['link']}")
        except HttpError as e:
            print(f"❌ Error subiendo {os.path.basename(p)}: {e}")

    # Guardar fichero de enlaces para que el email lo integre
    with open(LINKS_TXT, "w", encoding="utf-8") as f:
        f.write(f"# Enlaces Drive · {datetime.now():%Y-%m-%d %H:%M}\n")
        for u in uploads:
            f.write(f"{u['name']} | {u['link']}\n")

    print(f"✅ Subida completada. Enlaces guardados en {LINKS_TXT}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(f"Error inesperado: {e}")
