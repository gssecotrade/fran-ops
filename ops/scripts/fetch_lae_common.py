# -*- coding: utf-8 -*-
import re
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LAE-Bot/1.0; +https://github.com)"
}
TIMEOUT = 30

# === FUENTES EDITABLES ==========================================
# Si ves 404 en Actions, abre el enlace en el navegador y copia la URL correcta.
# Dejo valores “probables”; cámbialos si la web usa otro slug.
SOURCES = {
    "PRIMITIVA": [
        "https://www.lotoideas.com/historico-primitiva/",
    ],
    "BONOLOTO": [
        "https://www.lotoideas.com/historico-bonoloto/",
    ],
    "GORDO": [
        "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva/",
    ],
    "EURO": [
        "https://www.lotoideas.com/historico-euromillones/",
    ],
}
# ===============================================================

# Si los orígenes fallan podemos dejar aquí una semilla en el repo (docs/api/*.json)
SEED_LATEST_URL = "https://raw.githubusercontent.com/gssecotrade/fran-ops/main/docs/api/seed_latest.json"
SEED_HISTORIC_URL = "https://raw.githubusercontent.com/gssecotrade/fran-ops/main/docs/api/seed_historic.json"


def http_get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def parse_date_any(s: str) -> Optional[str]:
    """Convierte '26/09/2025' o '2025-09-26' a 'YYYY-MM-DD'."""
    s = s.strip()
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', s)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        y, mth, d = m.groups()
        return f"{y}-{mth}-{d}"
    return None


def parse_table_lotoideas(html: str, game: str) -> List[Dict[str, Any]]:
    """
    Parser tolerante para tablas tipo:
      FECHA | N1 | N2 | N3 | N4 | N5 | N6 | Compl | R / Estrellas...
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tr")
    out: List[Dict[str, Any]] = []
    if not rows or len(rows) < 2:
        return out

    header = [th.get_text(strip=True).upper() for th in rows[0].find_all(["th","td"])]
    # índices por nombre aproximado
    def col_idx(names):
        for i, h in enumerate(header):
            for n in names:
                if n in h:
                    return i
        return -1

    i_fecha = col_idx(["FECHA", "DATE"])
    i_n = [col_idx([f"N{i}", f"B{i}", f"NUM{i}"]) for i in range(1,7)]
    i_compl = col_idx(["COMPLEMENTARIO","COMPL","CLAVE"])
    i_r = col_idx(["REINTEGRO","R"])
    i_e1 = col_idx(["E1","ESTRELLA 1","STAR 1"])
    i_e2 = col_idx(["E2","ESTRELLA 2","STAR 2"])

    for tr in rows[1:]:
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if not tds:
            continue
        date_raw = tds[i_fecha] if 0 <= i_fecha < len(tds) else ""
        date = parse_date_any(date_raw)
        if not date:
            continue

        def safe_num(i):
            if 0 <= i < len(tds):
                m = re.search(r'\d+', tds[i])
                return int(m.group()) if m else None
            return None

        nums = [safe_num(i) for i in i_n if i >= 0]
        nums = [x for x in nums if isinstance(x, int)]
        # Algunas loterías tienen 5 números + extras
        if len(nums) < 5:
            continue

        compl = safe_num(i_compl) if i_compl >= 0 else None
        r = safe_num(i_r) if i_r >= 0 else None
        e1 = safe_num(i_e1) if i_e1 >= 0 else None
        e2 = safe_num(i_e2) if i_e2 >= 0 else None

        draw = {"game": game, "date": date, "numbers": nums}
        if compl is not None:
            draw["complementario"] = compl
        if r is not None:
            draw["reintegro"] = r
        if e1 is not None or e2 is not None:
            stars = []
            if e1 is not None: stars.append(e1)
            if e2 is not None: stars.append(e2)
            draw["estrellas"] = stars

        out.append(draw)

    return out


def fetch_game(game: str) -> List[Dict[str, Any]]:
    errors = []
    for url in SOURCES.get(game, []):
        try:
            logging.info(f"[fetch] {game} -> {url}")
            html = http_get(url)
            data = parse_table_lotoideas(html, game)
            if data:
                return data
            else:
                errors.append(f"{game}: parser vacío en {url}")
        except Exception as e:
            errors.append(f"{game}: {e}")
            time.sleep(1)

    # último recurso: nada
    logging.warning(f"[warn] {game} sin datos. Errores: {errors}")
    return []


def build_payload(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": results,
        "errors": [],
    }


def safe_write_json(path: str, payload: Dict[str, Any]) -> None:
    """
    Solo escribe si hay resultados; si no hay, mantiene el fichero previo
    (evita publicar JSON vacío en producción).
    """
    if not payload.get("results"):
        logging.warning(f"[guard] No hay resultados; no se sobreescribe {path}")
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logging.info(f"[done] Escrito {len(payload['results'])} sorteos en {path}")
