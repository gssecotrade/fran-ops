#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# --- Lee variables de entorno (definidas en el workflow como Secrets) ---
CLIENT_ID     = os.getenv("GDRIVE_OAUTH_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GDRIVE_OAUTH_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("GDRIVE_OAUTH_REFRESH_TOKEN", "")
FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "").strip()

TOKEN_URI = "https://oauth2.googleapis.com/token"

def get_service():
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError("Faltan CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN en env")
    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri=TOKEN_URI,          # IMPORTANTE para poder refrescar token
    )
    return build("drive", "v3", credentials=creds)

def ensure_folder(svc, folder_id: str):
    if not folder_id:
        raise RuntimeError("GDRIVE_FOLDER_ID vacío")
    try:
        meta = svc.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,driveId,parents",
            supportsAllDrives=True,
        ).execute()
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            raise RuntimeError(f"GDRIVE_FOLDER_ID no es una carpeta: {meta}")
        print(f"✓ Carpeta destino: {meta['name']} ({meta['id']})")
    except HttpError as e:
        raise RuntimeError(f"GDRIVE_FOLDER_ID inválido o sin acceso: {folder_id} · {e}")

def upload_one(svc, zip_path: Path, parent_id: str):
    file_metadata = {"name": zip_path.name, "parents": [parent_id]}
    media = MediaFileUpload(
        zip_path.as_posix(),
        mimetype="application/zip",
        resumable=True,                # Subida robusta
    )
    request = svc.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink,parents",
        supportsAllDrives=True,
    )
    try:
        # next_chunk maneja la subida por partes
        resp = None
        while resp is None:
            status, resp = request.next_chunk()
        print(f"↑ Subido: {zip_path.name}  → {resp.get('webViewLink')}")
        return resp
    except HttpError as e:
        print(f"✗ Error subiendo {zip_path.name}: {e}")
        return None

def main():
    base = Path(__file__).resolve().parents[2]
    dist = base / "dist"
    if not dist.exists():
        print(f"No existe {dist}")
        return

    svc = get_service()
    ensure_folder(svc, FOLDER_ID)

    zips = sorted(dist.glob("*.zip"))
    print(f"→ Subiendo ZIPs desde {dist} a Drive folder …")
    links_out = []

    for z in zips:
        resp = upload_one(svc, z, FOLDER_ID)
        if resp:
            links_out.append(f"{resp['name']}\t{resp.get('webViewLink','')}")

    if links_out:
        out = dist / "drive_links.txt"
        out.write_text("\n".join(links_out), encoding="utf-8")
        print(f"✓ Enlaces guardados en {out}")
    else:
        print("No se subió ningún archivo.")

if __name__ == "__main__":
    main()
