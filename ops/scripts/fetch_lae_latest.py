#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_lae_latest.py
-------------------------------------------------
Descarga el ÚLTIMO sorteo de PRIMITIVA, BONOLOTO,
EL GORDO y EUROMILLONES desde la web de LAE,
con fallback a un proxy sin JS y SIEMPRE
genera un JSON de salida aunque falle algún juego.

Salida: {"generated_at": "...Z", "results": [...], "errors":[...]}
Uso:    python ops/scripts/fetch_lae_latest.py --out docs/api/lae_latest.json
"""

import sys, re, json, argparse, datetime, time
from typing import Dict, Any, List, Tuple, Optional

try:
    import requests
    from bs4 import BeautifulSoup  # type: ignore
except Exception as e:
    # Emergencia: si faltan deps, devolvemos placeholder pero NO reventamos el job
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(json.dumps({
        "generated_at": now,
        "results": [],
        "errors": [f"IMPORT_ERROR:{repr(e)}"]
    }, ensure_ascii=False, indent=2))
    sys.exit(0)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
TIMEOUT = 25

# Endpoints “visibles” (suelen 403 sin cookies) y proxy texto
PAGES: Dict[str, Dict[str, str]] = {
    "PRIMITIVA": {
        "direct": "https://www.loteriasyapuestas.es/es/la-primitiva",
        "proxy":  "https://r.jina.ai/http://www.loteriasyapuestas.es/es/la-primitiva",
    },
    "BONOLOTO": {
        "direct": "https://www.loteriasyapuestas.es/es/bonoloto",
        "proxy":  "https://r.jina.ai/http://www.loteriasyapuestas.es/es/bonoloto",
    },
    "GORDO": {
        "direct": "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva",
        "proxy":  "https://r.jina.ai/http://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva",
    },
    "EURO": {
        "direct": "https://www.loteriasyapuestas.es/es/euromillones",
        "proxy":  "https://r.jina.ai/http://www.loteriasyapuestas.es/es/euromillones",
    },
}

HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def fetch_text(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Devuelve (texto, origen) usando directo y, si 403/errores, proxy."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.ok and r.text:
            return r.text, "direct"
        # Si nos devuelven 403/redirect raro, pasamos al proxy
    except requests.RequestException:
        pass
    # Fallback: proxy
    try:
        prox = PAGES  # acceso global
        # mapear la “direct” al “proxy”
        # (cuando nos pasan ya proxy, no duplicamos)
        if "r.jina.ai" in url:
            rp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if rp.ok and rp.text:
                return rp.text, "proxy"
        else:
            # buscar proxy equivalente
            for g, d in PAGES.items():
                if d["direct"] == url:
                    rp = requests.get(d["proxy"], headers=HEADERS, timeout=TIMEOUT)
                    if rp.ok and rp.text:
                        return rp.text, "proxy"
    except requests.RequestException:
        pass
    return None, None

# ---------- Parsers (robustos por regex) ----------

DATE_RX = re.compile(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})")

def parse_numbers_generic(text: str, how_many: int) -> List[int]:
    """Extrae números (hasta how_many) robustamente del texto (primero los más probables 1..50)."""
    # Priorizar números de 1-50 (quitan años/horas)
    nums = [int(x) for x in re.findall(r"\b([1-9]|[1-4]\d|50)\b", text)]
    out: List[int] = []
    for n in nums:
        if n not in out:
            out.append(n)
        if len(out) >= how_many:
            break
    return out

def normalize_date(s: str) -> Optional[str]:
    m = DATE_RX.search(s)
    if not m:
        return None
    d = m.group(1).replace('-', '/')
    # dd/mm/yyyy
    parts = d.split('/')
    if len(parts) != 3:
        return None
    dd, mm, yy = parts
    dd = dd.zfill(2); mm = mm.zfill(2)
    if len(yy) == 2:
        yy = ('20' + yy) if int(yy) < 70 else ('19' + yy)
    try:
        dt = datetime.datetime(int(yy), int(mm), int(dd))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None

def parse_primitiva(html: str) -> Dict[str, Any]:
    # Buscamos 6 + complementario + reintegro
    date = normalize_date(html) or ""
    # Heurística: 8 números (6 + 1 + 1)
    nums = parse_numbers_generic(html, 8)
    res = {
        "game": "PRIMITIVA",
        "date": date,
        "numbers": nums[:6],
        "complementario": nums[6] if len(nums) > 6 else None,
        "reintegro": nums[7] if len(nums) > 7 else None,
    }
    return res

def parse_bonoloto(html: str) -> Dict[str, Any]:
    date = normalize_date(html) or ""
    nums = parse_numbers_generic(html, 8)
    return {
        "game": "BONOLOTO",
        "date": date,
        "numbers": nums[:6],
        "complementario": nums[6] if len(nums) > 6 else None,
        "reintegro": nums[7] if len(nums) > 7 else None,
    }

def parse_gordo(html: str) -> Dict[str, Any]:
    # El Gordo: 5 números + número clave/reintegro
    date = normalize_date(html) or ""
    nums = parse_numbers_generic(html, 6)
    return {
        "game": "GORDO",
        "date": date,
        "numbers": nums[:5],
        "clave": nums[5] if len(nums) > 5 else None,
    }

def parse_euro(html: str) -> Dict[str, Any]:
    # Euromillones: 5 números + 2 estrellas
    date = normalize_date(html) or ""
    nums = parse_numbers_generic(html, 7)
    return {
        "game": "EURO",
        "date": date,
        "numbers": nums[:5],
        "estrellas": nums[5:7] if len(nums) >= 7 else [],
    }

PARSERS = {
    "PRIMITIVA": parse_primitiva,
    "BONOLOTO":  parse_bonoloto,
    "GORDO":     parse_gordo,
    "EURO":      parse_euro,
}

def build_one(game: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    urls = PAGES[game]
    html, origin = fetch_text(urls["direct"])
    if not html:
        html, origin = fetch_text(urls["proxy"])
    if not html:
        return None, f"{game}: fetch_failed"
    try:
        parsed = PARSERS[game](html)
        parsed["source"] = origin
        return parsed, None
    except Exception as e:
        return None, f"{game}: parse_failed {repr(e)}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for g in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        item, err = build_one(g)
        if item:
            # sanity checks mínimos: fecha y nºs no vacíos
            if not item.get("date"):
                errors.append(f"{g}: no_date")
            results.append(item)
        else:
            errors.append(err or f"{g}: unknown_error")

    payload = {
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": results,
        "errors": errors,
    }

    # Garantizamos escritura SIEMPRE
    try:
        out_path = args.out
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"OK -> {out_path}")
    except Exception as e:
        # Último salvavidas: volcamos por stdout para que el paso pueda redirigirlo
        print(json.dumps(payload, ensure_ascii=False))
        print(f"WRITE_ERROR:{repr(e)}", file=sys.stderr)

if __name__ == "__main__":
    main()
