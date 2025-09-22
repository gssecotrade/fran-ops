# Fran Ops — Loterías (Producción)

## ¿Qué hace el sistema?
Cada ejecución (manual o programada) realiza:
1. **Extrae** 17 hojas del Google Sheet maestro y las guarda como CSV.
2. **Normaliza** (limpieza, tipados, fechas, manifest y master CSV).
3. **DQ (Data Quality)**: comprueba integridad, fechas, duplicados y números fuera de rango.
4. **Empaqueta** en ZIPs (loterias, legales, marketing, hb_docs).
5. **Sube a Google Drive** (carpeta `ops-drops`) y **registra** enlaces y métricas en una **Hoja de Control**.
6. **Email resumen** con el estado, métricas y alertas de calidad.

## Salidas
- **Drive**: 3–4 ZIPs con todo el material del día.
- `dist/loterias_manifest_YYYYMMDD.csv`: inventario de ficheros normalizados.
- `dist/loterias_master.csv`: consolidado listo para BI.
- **Hoja de Control**: fila por ejecución con métricas + links a ZIPs y manifest.

## Operación
- **Manual**: GitHub → Actions → `Fran Ops — Scheduler` → **Run workflow**.
- **Automático**: todos los días a las 08:00 Madrid (cron UTC 06:00).
- **Alertas**: Si DQ = `FAIL`, el email lo indica y (opcional) se puede cortar la subida.

## Mantenimiento mínimo
- Si cambia el ID del Sheet o la carpeta destino de Drive, actualiza los **Secrets**:
  - `SHEETS_SPREADSHEET_ID`, `GDRIVE_FOLDER_ID`.
- Si cambian credenciales de Gmail para el resumen, actualiza:
  - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`, `SMTP_TO`.
- Para el control, usa `SHEETS_CONTROL_ID` (ver abajo).

## Requisitos de compartición
- **Service Account** (SA) compartida en Sheet y Drive:
  - Email SA: `fran-chatgpt-ops@app-primitiva.iam.gserviceaccount.com`
  - Comparte la Hoja de datos, la Hoja de control y la carpeta `ops-drops` al menos como **Editor**.
- **OAuth Gmail/Drive**: ya configurado en Secrets (refresh token).

## Estructura
