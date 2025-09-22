#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST="$BASE/dist"

timestamp="$(date +%Y%m%d_%H%M)"

# Loterías: si existe carpeta normalized/YYYY-MM-DD, zipeamos desde ahí
LOT_NORM_DIR="$(find "$DIST/loterias/normalized" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort | tail -n1 || true)"
if [[ -n "${LOT_NORM_DIR:-}" ]]; then
  (cd "$LOT_NORM_DIR" && zip -qr "$DIST/loterias_${timestamp}.zip" .)
else
  # fallback
  if [[ -d "$BASE/loterias/data" ]]; then
    (cd "$BASE/loterias" && zip -qr "$DIST/loterias_${timestamp}.zip" data)
  fi
fi

# hb_docs, legales, marketing (fallbacks; ajusta a tus carpetas reales si hace falta)
if [[ -d "$BASE/hb_docs" ]]; then
  (cd "$BASE" && zip -qr "$DIST/hb_docs_${timestamp}.zip" hb_docs)
fi
if [[ -d "$BASE/legales" ]]; then
  (cd "$BASE" && zip -qr "$DIST/legales_${timestamp}.zip" legales)
fi
if [[ -d "$BASE/marketing" ]]; then
  (cd "$BASE" && zip -qr "$DIST/marketing_${timestamp}.zip" marketing)
fi

echo "Listo en $DIST"
ls -lh "$DIST"/*.zip || true
