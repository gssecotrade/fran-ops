# ops/scripts/fetch_lae_common.py
# Utilidades comunes para scraping y normalización de resultados de LAE
# Estructura de salida:
# {
#   "generated_at": "...Z",
#   "results": [ { "game": "...", "date": "yyyy-mm-dd", ... }, ... ],
#   "errors": [ "mensaje", ... ]
# }

from __future__ import annotations
import re
import json
import datetime
from typing import List, Dict, Any, Optional

# ---------- Normalización de campos ----------

_DD_MM_YYYY = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4})\s*$")
_YYYY_MM_DD = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")


def today_utc() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


def norm_date(s: str) -> str:
    """Convierte 'dd/mm/yyyy' o 'yyyy-mm-dd' a 'yyyy-mm-dd'. Si no reconoce, devuelve tal cual."""
    if not s:
        return s
    s = s.strip()
    m = _DD_MM_YYYY.match(s)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    m = _YYYY_MM_DD.match(s)
    if m:
        return s[:10]
    return s


def to_int(x) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return None


# ---------- Serialización ----------

def write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------- Empaquetadores por juego ----------

def pack_primitiva_bonoloto(row: List[str], game: str) -> Optional[Dict[str, Any]]:
    """
    Tabla Lotoideas (PRIMITIVA/BONOLOTO), cabeceras típicas:
      FECHA | 01 | 02 | 03 | 04 | 05 | 06 | COMP. | R.
    """
    if len(row) < 9:
        return None
    date = norm_date(row[0])
    nums = [to_int(x) for x in row[1:7]]
    comp = to_int(row[7])
    rein = to_int(row[8])
    if any(n is None for n in nums) or comp is None or rein is None:
        return None
    return {
        "game": game,
        "date": date,
        "numbers": nums,
        "complementario": comp,
        "reintegro": rein,
        "source": "lotoideas",
    }


def pack_gordo(row: List[str]) -> Optional[Dict[str, Any]]:
    """
    Tabla Lotoideas (EL GORDO), cabeceras típicas:
      FECHA | 01 | 02 | 03 | 04 | 05 | CLAVE
    """
    if len(row) < 7:
        return None
    date = norm_date(row[0])
    nums = [to_int(x) for x in row[1:6]]
    clave = to_int(row[6])
    if any(n is None for n in nums) or clave is None:
        return None
    return {
        "game": "GORDO",
        "date": date,
        "numbers": nums,
        "clave": clave,
        "source": "lotoideas",
    }


def pack_euro(row: List[str]) -> Optional[Dict[str, Any]]:
    """
    Tabla Lotoideas (EUROMILLONES), cabeceras típicas:
      FECHA | 01 | 02 | 03 | 04 | 05 | E1 | E2
    """
    if len(row) < 8:
        return None
    date = norm_date(row[0])
    nums = [to_int(x) for x in row[1:6]]
    e1 = to_int(row[6])
    e2 = to_int(row[7])
    if any(n is None for n in nums) or e1 is None or e2 is None:
        return None
    return {
        "game": "EURO",
        "date": date,
        "numbers": nums,
        "estrellas": [e1, e2],
        "source": "lotoideas",
    }


# ---------- Utilidades de negocio ----------

def latest_by_game(all_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Devuelve el último sorteo por juego, ordenado PRIM->BONO->GORDO->EURO."""
    out: Dict[str, Dict[str, Any]] = {}
    for g in ("PRIMITIVA", "BONOLOTO", "GORDO", "EURO"):
        subset = [r for r in all_results if r.get("game") == g and r.get("date")]
        if not subset:
            continue
        latest = sorted(subset, key=lambda x: x["date"], reverse=True)[0]
        out[g] = latest

    ordered = []
    for g in ("PRIMITIVA", "BONOLOTO", "GORDO", "EURO"):
        if g in out:
            ordered.append(out[g])
    return ordered
