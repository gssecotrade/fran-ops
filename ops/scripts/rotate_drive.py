#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rotate Drive files:
- Borra ficheros antiguos en la carpeta GDRIVE_FOLDER_ID manteniendo retención.
- Mantiene al menos el más reciente de cada día (por createdTime).
- Usa OAuth (mismos secrets que upload_to_gdrive.py).

Variables de entorno:
  GDRIVE_OAUTH_CLIENT_ID
  GDRIVE_OAUTH_CLIENT_SECRET
  GDRIVE_OAUTH_REFRESH_TOKEN
  GDRIVE_FOLDER_ID
  RETENTION_DAYS (opcional, por defecto 14)
  ROTATE_DRY_RUN (opcional: "1" no borra, solo lista)
"""

import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


def build_service():
    creds = Credentials(
        None,
        refresh_token=os.environ["GDRIVE_OAUTH_REFRESH_TOKEN"],
        client_id=os.environ["GDRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_OAUTH_CLIENT_SECRET"],
    )
    return build("drive", "v3", credentials=creds)

def list_files(service, folder_id):
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id,name,createdTime,size)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def main():
    svc = build_service()

    folder_id = os.environ["GDRIVE_FOLDER_ID"].strip()
    retention_days = int(os.environ.get("RETENTION_DAYS", "14"))
    dry_run = os.environ.get("ROTATE_DRY_RUN", "") == "1"

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    files = list_files(svc, folder_id)
    if not files:
        print("Rotate: no hay ficheros en la carpeta, nada que hacer.")
        return

    # Agrupar por día (UTC) y decidir cuál conservar (el más reciente del día)
    by_day = defaultdict(list)
    for f in files:
        ctime = datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
        day_key = ctime.date().isoformat()
        by_day[day_key].append((ctime, f))

    keep_ids = set()
    for day_key, lst in by_day.items():
        lst.sort(key=lambda t: t[0], reverse=True)  # más nuevo primero
        keep_ids.add(lst[0][1]["id"])               # conserva el más nuevo del día

    to_delete = []
    for f in files:
        fid = f["id"]
        ctime = datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
        if ctime < cutoff and fid not in keep_ids:
            to_delete.append(f)

    print(f"Rotate: total en carpeta={len(files)} | a borrar={len(to_delete)} | retención={retention_days} días")
    for f in to_delete:
        print(f" - DEL {f['id']}  {f['name']}  ({f['createdTime']})")

    if dry_run:
        print("Rotate: modo DRY-RUN, no se borrará nada.")
        return

    for f in to_delete:
        svc.files().delete(fileId=f["id"]).execute()
    print("Rotate: borrado completado.")

if __name__ == "__main__":
    main()
