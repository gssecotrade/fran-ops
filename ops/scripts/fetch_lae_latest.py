#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera docs/api/lae_latest.json con guard-rails:
- Estructura: { generated_at, results: [...], errors: [...] }
- NO publica sorteos sin fecha (items con "date" vacío o nulo se descartan)
- Si no hay ningún sorteo válido, publica results=[] y deja constancia en errors
- Preparado para ampliarse con scrapers reales; por ahora intenta obtener una
  fuente JSON ya existente (si existe) para mantener compatibilidad.

Salida: docs/api/lae_latest.json
"""

from __future__ import annotations
import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import requests
except Exception:
    print("Installing dependencies (requests)...", flush=True)
    os.system(f"{sys.executable} -m pip install --upgrade pip >/dev/null 2>&1")
    os.system(f"{sys.executable} -m pip install requests >/dev/null 2>&1")
    import requests  # type: ignore


# -------------------------
# Config
# -------------------------

OUT_PATH = "docs/api/lae_latest.json"

# Fuentes candidatas (se pueden añadir más)
CANDIDATE_SOURCES = [
    # 1) variable de entorno para pruebas/manual
    os.getenv("LAE_SOURCE_JSON") or "",
    # 2) rama principal del propio repo (si existiera un JSON previo)
    "https://raw.githubusercontent.com/gssecotrade/fran-ops/main/docs/api/lae_latest.json",
    # 3) GitHub Pages del repo (si está publicado)
    "https://gssecotrade.github.io/fran-ops/docs/api/lae_latest.json",
]


# -------------------------
# Utilidades
# -------------------------

def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_get_json(url: str, timeout: int = 20) -> Optional[Dict[str, Any]]:
    if not url:
        return None
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "fran-ops/gha"})
        if r.status_code >= 200 and r.status_code < 300:
            return r.json()
        return None
    except Exception:
        return None


def ensure_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)


def canonical_game_key(raw: Any) -> str:
    if not raw:
        return ""
    s = str(raw).strip().lower()
    if "primitiva" in s:
        return "PRIMITIVA"
    if "bonoloto" in s:
        return "BONOLOTO"
    if "gordo" in s:
        return "GORDO"
    if "euromillon" in s or ("euro" in s and "millon" in s):
        return "EURO"
    if raw in ("PRIMITIVA", "BONOLOTO", "GORDO", "EURO"):
        return str(raw)
    return ""


def results_from_any(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extrae un array de sorteos desde varias formas posibles."""
    if not isinstance(json_obj, dict):
        return []

    r = json_obj.get("results")
    if isinstance(r, list):
        return [x for x in r if isinstance(x, dict)]

    if isinstance(r, dict):
        out: List[Dict[str, Any]] = []
        for k, v in r.items():
            key = canonical_game_key(k)
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        item.setdefault("game", key or item.get("game"))
                        out.append(item)
            elif isinstance(v, dict):
                v.setdefault("game", key or v.get("game"))
                out.append(v)
        return out

    return []


# -------------------------
# Generación del payload
# -------------------------

def build_payload() -> Dict[str, Any]:
    errors: List[str] = []
    raw_items: List[Dict[str, Any]] = []

    # 1) Intentar fuentes candidatas (si alguna tuviera datos)
    for url in CANDIDATE_SOURCES:
        if not url:
            continue
        js = http_get_json(url)
        if not js:
            continue
        raw_items = results_from_any(js)
        if raw_items:
            break

    # 2) Guard-rail: filtrar items SIN fecha
    valid: List[Dict[str, Any]] = []
    for it in raw_items:
        date_val = it.get("date") or it.get("fecha") or it.get("draw_date")
        if date_val and str(date_val).strip() != "":
            valid.append(it)
        else:
            g = canonical_game_key(it.get("game"))
            errors.append(f"{g or 'UNKNOWN'}: no_date")

    # 3) Construir payload final
    payload = {
        "generated_at": utc_now_iso(),
        "results": valid,
        "errors": errors,
    }
    return payload


def main() -> int:
    payload = build_payload()
    ensure_dir(OUT_PATH)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Mensaje humano
    total = len(payload.get("results", []))
    print(f"[build] Escrito {OUT_PATH} con {total} sorteos válidos.")
    if payload.get("errors"):
        print("[build] Errores:", *payload["errors"], sep="\n  - ")

    return 0


if __name__ == "__main__":
    sys.exit(main())
