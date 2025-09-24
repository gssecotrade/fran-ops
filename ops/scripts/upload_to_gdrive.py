#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sube los ZIPs de ./dist a una carpeta de Google Drive usando OAuth (refresh token)
y realiza una limpieza automática de ZIPs antiguos en esa carpeta.

Requiere variables de entorno:
  GDRIVE_OAUTH_CLIENT_ID
  GDRIVE_OAUTH_CLIENT_SECRET
  GDRIVE_OAUTH_REFRESH_TOKEN
  GDRIVE_FOLDER_ID              -> carpeta destino (misma que ves en Drive)
Opcionales:
  GDRIVE_CLEANUP_DAYS           -> días para borrar ZIPs antiguos (defecto 30)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError


# -------------------------------
#  Helpers OAuth + Drive service
# -------------------------------
def build_service():
    cid  = os.environ.get("GDRIVE_OAUTH_CLIENT_ID")
    csec = os.environ.get("GDRIVE_OAUTH_CLIENT_SECRET")
    rref = os.environ.get("GDRIVE_OAUTH_REFRESH_TOKEN")
    if not (cid and csec and rref):
        raise RuntimeError("Faltan credenciales OAuth: define GDRIVE_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN")

    creds = Credentials(
        None,
        refresh_token=rref,
        client_id=cid,
        client_secret=csec,
        token_uri="https://oauth2.googleapis.com/token",
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def ensure_folder(service, folder_id: str) -> dict:
    """Comprueba que el folder existe y devuelve su metadata."""
    try:
        meta = (
            service.files()
            .get(
                fileId=folder_id,
                fields="id,name,driveId,parents",
                supportsAllDrives=True,
            )
            .execute()
        )
        print(f"✓ Carpeta destino: {meta.get('name')} ({meta.get('id')})")
        return meta
    except HttpError as e:
        raise RuntimeError(
            f"GDRIVE_FOLDER_ID inválido o no accesible: {folder_id} · {e}"
        )


# ---------------------------------
#  Limpieza de ficheros antiguos
# ---------------------------------
def parse_google_datetime(s: str) -> datetime:
    """Convierte timestamps ISO de Drive a datetime con tz UTC."""
    # viene tipo '2025-09-21T14:22:33.123Z'
    s = (s or "").rstrip("Z")
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def cleanup_old_files(service, folder_id: str, days: int = 30) -> int:
    """
    Borra del folder de Drive los archivos .zip con modifiedTime anterior al cutoff.
    Devuelve nº de ficheros eliminados.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, days))
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    deleted = 0
    page_token = None

    print(f"🧹 Limpieza Drive: ZIPs con más de {days} días (cutoff={cutoff.isoformat()})")

    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,modifiedTime,mimeType)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token,
                pageSize=100,
            )
            .execute()
        )
        for f in resp.get("files", []):
            name = f.get("name", "")
            if not name.lower().endswith(".zip"):
                continue
            mt = parse_google_datetime(f.get("modifiedTime"))
            if mt < cutoff:
                try:
                    service.files().delete(fileId=f["id"]).execute()
                    print(f"   🗑️  Deleted {name} (modified: {mt.isoformat()})")
                    deleted += 1
                except Exception as e:
                    print(f"   ⚠️  Delete error {name}: {e}")

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"🧹 Limpieza completada. Eliminados: {deleted}")
    return deleted


# -------------------------------
#  Subida de ZIPs
# -------------------------------
def upload_zip(service, folder_id: str, path: Path) -> str:
    file_metadata = {"name": path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(path), mimetype="application/zip", resumable=True)

    req = service.files().create(
        body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
    )
    resp = req.execute()
    file_id = resp["id"]

    # crear link de visualización
    link = f"https://drive.google.com/file/d/{file_id}/view?usp=drivesdk"
    print(f"↑ Subido: {path.name}  → {link}")
    return link


def upload_all_from_dist(service, folder_id: str, dist_dir: Path) -> list:
    links = []
    for p in sorted(dist_dir.glob("*.zip")):
        try:
            link = upload_zip(service, folder_id, p)
            links.append((p.name, link))
        except Exception as e:
            print(f"⚠️ Error subiendo {p.name}: {e}")
    if links:
        out = dist_dir / "drive_links.txt"
        with out.open("w", encoding="utf-8") as fh:
            for name, link in links:
                fh.write(f"{name}\t{link}\n")
        print(f"✓ Enlaces guardados en {out}")
    return links


# -------------------------------
#  Main
# -------------------------------
def main():
    dist_dir = Path("dist").resolve()
    if not dist_dir.exists():
        print("No existe ./dist; nada que subir.")
        return

    folder_id = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        print("❌ Falta GDRIVE_FOLDER_ID en entorno")
        sys.exit(0)

    service = build_service()
    ensure_folder(service, folder_id)

    # Subir ZIPs que existan en ./dist
    print(f"→ Subiendo ZIPs desde {dist_dir} a Drive folder …")
    upload_all_from_dist(service, folder_id, dist_dir)

    # --- NUEVO: limpieza automática tras la subida
    try:
        cleanup_days = int(os.environ.get("GDRIVE_CLEANUP_DAYS", "30"))
    except Exception:
        cleanup_days = 30
    try:
        cleanup_old_files(service, folder_id, cleanup_days)
    except Exception as e:
        print("⚠️ Limpieza omitida:", repr(e))


if __name__ == "__main__":
    main()
