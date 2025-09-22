import os
import sys
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

DIST_DIR = "dist"

def build_service_from_oauth():
    creds = Credentials(
        None,
        refresh_token=os.environ["GDRIVE_OAUTH_REFRESH_TOKEN"],
        client_id=os.environ["GDRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_OAUTH_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
    )
    return build("drive", "v3", credentials=creds)

def ensure_folder(service, folder_id):
    try:
        return service.files().get(
            fileId=folder_id,
            fields="id,name,mimeType,parents",
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        raise RuntimeError(f"GDRIVE_FOLDER_ID inválido: {folder_id} · {e}")

def upload_file(service, folder_id, filepath):
    filename = os.path.basename(filepath)
    media = MediaFileUpload(filepath, resumable=True)
    file_metadata = {"name": filename, "parents": [folder_id]}
    uploaded = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,webViewLink",
        supportsAllDrives=True
    ).execute()
    return uploaded

def main():
    folder_id = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        print("❌ Falta GDRIVE_FOLDER_ID en entorno", file=sys.stderr)
        sys.exit(1)

    service = build_service_from_oauth()
    ensure_folder(service, folder_id)

    links = []
    for fname in os.listdir(DIST_DIR):
        if not fname.endswith(".zip"):
            continue
        fpath = os.path.join(DIST_DIR, fname)
        print(f"↑ Subiendo: {fname} …")
        meta = upload_file(service, folder_id, fpath)
        url = meta["webViewLink"]
        print(f"→ OK: {fname} → {url}")
        links.append(f"{fname}\t{url}")

    with open(os.path.join(DIST_DIR, "drive_links.txt"), "w") as f:
        f.write("\n".join(links))

if __name__ == "__main__":
    main()
