#!/usr/bin/env python3
import os
import glob
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DIST = os.path.join(BASE, "dist")

# === OAuth por Refresh Token (sin cuota de Service Account) ===
CLIENT_ID     = os.getenv("GDRIVE_OAUTH_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GDRIVE_OAUTH_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("GDRIVE_OAUTH_REFRESH_TOKEN", "")

# Carpeta destino (Shared Drive folder / MyDrive folder)
FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "")

# Scopes mínimos para subir ficheros
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def get_drive_service():
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOKEN):
        raise RuntimeError("Faltan GDRIVE_OAUTH_CLIENT_ID / _CLIENT_SECRET / _REFRESH_TOKEN en variables de entorno.")

    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES,
        token_uri="https://oauth2.googleapis.com/token",
    )
    # 'supportsAllDrives' lo forzamos en cada llamada
    return build("drive", "v3", credentials=creds)

def ensure_folder(service, parent_id):
    # Chequeo muy básico: la carpeta destino debe existir (si no, error claro)
    try:
        service.files().get(fileId=parent_id, fields="id, name, driveId, parents", supportsAllDrives=True).execute()
    except HttpError as e:
        raise RuntimeError(f"GDRIVE_FOLDER_ID inválido o no compartido con tu cuenta: {parent_id} · {e}")

def upload_file(service, filepath, folder_id):
    name = os.path.basename(filepath)

    file_metadata = {
        "name": name,
        "parents": [folder_id],
    }
    media = MediaFileUpload(filepath, resumable=True)

    # supportsAllDrives=True permite Shared Drives; si es Mi unidad, también funciona.
    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name, webViewLink, parents",
        supportsAllDrives=True,
    ).execute()

    return created  # dict con id, name, webViewLink

def main():
    print(f"→ Subiendo ZIPs desde {DIST} a Drive folder {FOLDER_ID} …")

    if not FOLDER_ID:
        print("⚠️ Falta GDRIVE_FOLDER_ID; nada que subir.")
        return

    zips = sorted(glob.glob(os.path.join(DIST, "*.zip")))
    if not zips:
        print("⚠️ No hay ZIPs en dist/; nada que subir.")
        return

    service = get_drive_service()
    ensure_folder(service, FOLDER_ID)

    links_path = os.path.join(DIST, "drive_links.txt")
    links = []

    for z in zips:
        try:
            created = upload_file(service, z, FOLDER_ID)
            link = f"{created.get('name')} -> {created.get('webViewLink')}"
            print(f"✅ Subido: {link}")
            links.append(link)
        except Exception as e:
            print(f"❌ Error subiendo {z}: {e}")

    # Guardamos enlaces para el resumen
    if links:
        with open(links_path, "w", encoding="utf-8") as f:
            f.write("Archivos subidos · " + datetime.now().strftime("%Y-%m-%d %H:%M") + "\n")
            for l in links:
                f.write(l + "\n")
        print(f"✅ Subida completada. Enlaces guardados en {links_path}")
    else:
        print("⚠️ No se subió ningún archivo.")

if __name__ == "__main__":
    main()
