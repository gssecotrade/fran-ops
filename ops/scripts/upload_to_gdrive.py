import os
import glob
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload   # <-- IMPORT NECESARIO
from google.oauth2 import service_account

# === Configuración desde Secrets ===
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SA_JSON", "ops/.secrets/google-sa.json")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
DRIVE_ID = os.getenv("GDRIVE_DRIVE_ID", "")  # ID de la Unidad Compartida (opcional pero recomendado)

# === Autenticación ===
SCOPES = ["https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=creds)

def upload_file(file_path: str, folder_id: str):
    """Sube un fichero a una CARPETA dentro de una Unidad Compartida."""
    meta = {
        "name": os.path.basename(file_path),
        "parents": [folder_id],
    }
    # Para Shared Drives es buena práctica incluir driveId
    if DRIVE_ID:
        meta["driveId"] = DRIVE_ID

    media = MediaFileUpload(file_path, resumable=True)
    file = (
        service.files()
        .create(
            body=meta,
            media_body=media,
            fields="id,name,webViewLink,parents",
            supportsAllDrives=True,  # imprescindible en Shared Drives
        )
        .execute()
    )
    print(f"✅ Subido: {file['name']} → {file.get('webViewLink','')}")
    return file

if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    dist = os.path.join(base, "dist")
    paths = sorted(glob.glob(os.path.join(dist, "*.zip")))
    print(f"→ Subiendo {len(paths)} ficheros a Drive folder {GDRIVE_FOLDER_ID} …")

    links_path = os.path.join(dist, "drive_links.txt")
    with open(links_path, "w") as f:
        for p in paths:
            try:
                up = upload_file(p, GDRIVE_FOLDER_ID)
                f.write(f"{up['name']}: {up.get('webViewLink','')}\n")
            except Exception as e:
                print(f"❌ Error subiendo {p}: {e}")
