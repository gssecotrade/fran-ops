#!/bin/bash
set -e
cd "$(dirname "$0")"/../..
mkdir -p dist
ts=$(date +%Y%m%d_%H%M)
zip -rq dist/hb_docs_${ts}.zip hb || true
zip -rq dist/legales_${ts}.zip legales/salida || true
zip -rq dist/marketing_${ts}.zip marketing || true
zip -rq dist/loterias_${ts}.zip loterias || true
echo 'Listo en ./dist'
