#!/bin/bash
cd "$(dirname "$0")"/..
source .env 2>/dev/null || true
python3 scripts/sheets_to_csv.py || true
python3 scripts/normalize_legales.py || true
bash scripts/zip_all.sh || true
python3 scripts/upload_to_gdrive.py dist "$GDRIVE_FOLDER_ID" || true
python3 scripts/send_summary_email.py || true
echo OK
