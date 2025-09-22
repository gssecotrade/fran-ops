#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

DIST_DIR = Path(__file__).resolve().parents[2] / "dist"

# VARIABLES DE ENTORNO
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()

# OAuth (NO pasamos scopes aquí; el refresh token ya los trae)
OAUTH_CLIENT_ID = os.getenv("GDRIVE_OAUTH_CLIENT_ID", "").strip()
OAUTH_CLIENT_SECRET = os.getenv("GDRIVE_OAUTH_CLIENT_SECRET", "").strip()
OAUTH_REFRESH_TOKEN = os.getenv("GDRIVE_OAUTH_REFRESH_TOKEN", "").strip()

TOKEN_URI = "https://oauth2.googleapis.com/token"


def build_drive_service():
    """
    Crea el servicio Drive usando OAuth Refresh Token.
    IMPORTANTE: NO pasar scopes al crear Credentials; si los scopes
    del refresh token y los de aquí difieren, Google responde invalid_scope.
    """
    if not (OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET and OAUTH_REFRESH_TOKEN):
        raise RuntimeError(
            "Faltan credenciales OAuth en variables: "
            "GDRIVE_OAUTH_CLIENT_ID / GDRIVE_OAUTH_CLIENT_SECRET / GDRIVE_OAUTH_REFRESH_TOKEN"
        )

    creds = Credentials(
        token=None,
        refresh_token=OAUTH_REFRESH_TOKEN,
        client_id=OAUTH_CLIENT_ID,
        client_secret=OAUTH_CLIENT_SECRET,
        token_uri=TOKEN_URI,
    )
    # build() refrescará automáticamente si es necesario
    return build("drive", "v3", credentials=creds)


def ensure_folder(service, folder_id: str):
    """Verifica que el folder exista y sea accesible."""
    try:
        meta = (
            service.files()
            .get(
                fileId=folder_id,
                fields="id,name,mimeType,parents,driveId",
                supportsAllDrives=True,
            )
            .execute()
        )
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            raise RuntimeError(f"El id {folder_id} no es una carpeta en Drive.")
        print(f"✓ Carpeta destino: {meta['name']} ({meta['id']})")
    except HttpError as e:
        raise RuntimeError(
            f"GDRIVE_FOLDER_ID inválido o no accesible: {folder_id} · {e}"
        )


def upload_file(service, folder_id: str, file_path: Path) -> Optional[str]:
    """Sube un archivo a la carpeta destino. Devuelve el fileId si ok."""
    body = {"name": file_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(file_path), resumable=True)

    try:
        created = (
            service.files()
            .create(
                body=body,
                media_body=media,
                fields="id,name,webViewLink,parents",
                supportsAllDrives=True,
            )
            .execute()
        )
        print(f"  ↑ Subido: {created['name']} → {created.get('webViewLink','')}")
        return created["id"]
    except HttpError as e:
        print(f"  ❌ Error subiendo {file_path.name}: {e}")
        return None


def main():
    if not FOLDER_ID:
        print("Falta GDRIVE_FOLDER_ID.")
        sys.exit(1)

    if not DIST_DIR.exists():
        print(f"No existe la carpeta de salida: {DIST_DIR}")
        sys.exit(0)

    print(f"→ Subiendo ZIPs desde {DIST_DIR} a Drive folder *** …")

    service = build_drive_service()
    ensure_folder(service, FOLDER_ID)

    # Sube todos los ZIP del dist
    zips = sorted(DIST_DIR.glob("*.zip"))
    if not zips:
        print("No hay ZIPs para subir.")
        return

    for z in zips:
        upload_file(service, FOLDER_ID, z)

    print("✓ Subida completada.")


if __name__ == "__main__":
    main()
