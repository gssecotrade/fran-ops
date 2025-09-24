# Fran Ops · Runbook

## Lanzar manualmente
- GitHub → Actions → **Fran Ops — Scheduler** → Run workflow.

## Qué comprobar
- Email resumen (asunto `[Fran Ops] Resumen …`) debe llegar en <2 min.
- Panel DQ: `Settings → Pages` o `<https://<tu-usuario>.github.io/fran-ops/>`  
  Debe mostrar:
  - Estado **OK/WARN/FAIL**.
  - Enlaces a ZIPs en Drive.
  - Manifest y Master CSV.

## Errores frecuentes
- **Drive OAuth `invalid_scope`**: refrescar token con `get_refresh_token.py` y actualizar secretos.
- **404 Folder**: `GDRIVE_FOLDER_ID` no corresponde a la carpeta compartida → verifica ID y permisos.
- **Email**: SMTP_* secretos incompletos.

## Mantenimiento
- **Limpieza Drive**: automática (>30 días). Ajusta `GDRIVE_CLEANUP_DAYS` en el workflow.
- **DQ reglas**: `ops/scripts/dq_loterias.py`.

## Contactos
- Owner repo: @gssecotrade
