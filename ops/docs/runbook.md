1) Extrae datos de Sheets → CSV (ops/scripts/sheets_to_csv.py)
2) Normaliza loterías y genera manifest/master (ops/scripts/normalize_loterias.py)
3) Data Quality (ops/scripts/dq_loterias.py) → dist/dq_report.txt
4) ZIPs en dist/ y subida a Drive (ops/scripts/upload_to_gdrive.py)
5) Genera panel DQ (make_report.py) → docs/index.html

GitHub → Actions → *Fran Ops — Scheduler* → Run workflow
- Si falla con invalid_scope o 404 de carpeta:
  - Revisa secretos: GDRIVE_OAUTH_CLIENT_ID, GDRIVE_OAUTH_CLIENT_SECRET, GDRIVE_OAUTH_REFRESH_TOKEN, GDRIVE_FOLDER_ID.
  - Comparte la carpeta destino en Drive con el *service account* si aplica.
- Secretos SMTP: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO.

- El script elimina ZIPs antiguos automáticamente (ver ops/scripts/upload_to_gdrive.py).
