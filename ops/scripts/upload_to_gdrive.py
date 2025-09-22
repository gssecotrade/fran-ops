import os
import glob
from googleapiclient.discovery import build
from google.oauth2 import service_account

# === Configuración desde Secrets ===
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SA_JSON", "ops/.secrets/google-sa.json")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
DRIVE_ID = os.getenv("GDRIVE_DRIVE_ID", "")  # Nuevo: ID de la unidad compartida

# === Autenticación ===
SCOPES = ["https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=creds)

def upload_file(file_path, folder_id):
    file_metadata = {
        "name": os.path.basename(file_path),
        "parents": [folder_id]
    }
    if DRIVE_ID:
        file_metadata["driveId"] = DRIVE_ID

    media = MediaFileUpload(file_path, resumable=True)
    file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink, parents",
            supportsAllDrives=True
        )
        .execute()
    )
    print(f"✅ Subido: {file['name']} ({file['webViewLink']})")
    return file

if __name__ == "__main__":
    dist = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dist"))
    files = glob.glob(os.path.join(dist, "*"))
    print(f"→ Subiendo {len(files)} ficheros a Drive folder {GDRIVE_FOLDER_ID} …")

    links_path = os.path.join(dist, "drive_links.txt")
    with open(links_path, "w") as f:
        for file_path in files:
            try:
                uploaded = upload_file(file_path, GDRIVE_FOLDER_ID)
                f.write(f"{uploaded['name']}: {uploaded['webViewLink']}\n")
            except Exception as e:
                print(f"❌ Error subiendo {file_path}: {e}")
