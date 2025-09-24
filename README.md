## Panel Data Quality
[![pages](https://img.shields.io/badge/DQ%20Panel-live-blue)](https://gssecotrade.github.io/fran-ops/)

cat > README.md <<'EOF'
# Fran Ops — Loterías (Producción)

## Qué hace el sistema
1) Extrae 17 hojas del Google Sheet maestro a CSV.
2) Normaliza (limpieza, tipados, fechas, manifest y master CSV).
3) Data Quality (duplicados, rangos de fechas, conteos).
4) Empaqueta en ZIPs (loterias, legales, marketing, hb_docs).
5) Sube a Google Drive (carpeta ops-drops) y guarda enlaces/métricas.
6) Envía email resumen con estado y alertas.

## Salidas
- Drive: 3–4 ZIPs por ejecución.
- dist/loterias_manifest_YYYYMMDD.csv: inventario.
- dist/loterias_master.csv: consolidado BI.
- Hoja de Control: una fila por ejecución con métricas + links.

## Operación
- Manual: GitHub → Actions → “Fran Ops — Scheduler” → Run workflow.
- Automático: 08:00 Madrid (cron UTC 06:00).
- Si DQ = FAIL, el email lo indica (y puede cortar subida si se configura).

## Secrets mínimos (GitHub → Settings → Secrets and variables → Actions)
- SHEETS_SPREADSHEET_ID
- GDRIVE_FOLDER_ID
- GOOGLE_SA_JSON_BASE64
- GDRIVE_OAUTH_CLIENT_ID
- GDRIVE_OAUTH_CLIENT_SECRET
- GDRIVE_OAUTH_REFRESH_TOKEN
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO, SUMMARY_EMAIL
- (Opcional) SHEETS_CONTROL_ID para la Hoja de Control

## Requisitos de compartición
- Service Account (SA) con acceso Editor al Sheet maestro, a la Hoja de Control y a la carpeta de Drive:
  fran-chatgpt-ops@app-primitiva.iam.gserviceaccount.com
- OAuth (los 3 secrets anteriores) ya configurado.

## Estructura
ops/
  scripts/
    sheets_to_csv.py
    normalize_loterias.py
    dq_loterias.py
    upload_to_gdrive.py
    update_control_sheet.py
    send_summary_email.py
dist/
  loterias_manifest_YYYYMMDD.csv
  loterias_master.csv
  *.zip

## Hoja de Control (columnas sugeridas)
timestamp_utc, dag_run_id, csv_count, rows_total, dq_status, manifest_path, master_path,
zip_loterias, zip_legales, zip_marketing, zip_hb_docs

## Orden del pipeline
1) sheets_to_csv.py
2) normalize_loterias.py
3) dq_loterias.py
4) zip_all.sh
5) upload_to_gdrive.py
6) update_control_sheet.py
7) send_summary_email.py
EOF
