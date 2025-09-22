import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def build_service():
    creds = Credentials(
        None,
        refresh_token=os.environ["GDRIVE_OAUTH_REFRESH_TOKEN"],
        client_id=os.environ["GDRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_OAUTH_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
    )
    return build("drive", "v3", credentials=creds)

def rotate_folder_content(folder_id):
    service = build_service()
    files = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name)", supportsAllDrives=True
    ).execute().get("files", [])
    print(f"Archivos en {folder_id}: {[f['name'] for f in files]}")

if __name__ == "__main__":
    fid = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not fid:
        raise RuntimeError("❌ Falta GDRIVE_FOLDER_ID")
    rotate_folder_content(fid)
