# -*- coding: utf-8 -*-
import re
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LAE-Bot/1.0; +https://github.com)"
}
TIMEOUT = 30
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ========= Sitio base (autodiscovery sobre este dominio) =========
BASE = "https://www.lotoideas.com"

# Palabras clave por juego para localizar la URL correcta del histórico
KEYWORDS = {
    "PRIMITIVA": ["primitiva"],
    "BONOLOTO": ["bonoloto", "bono-loto"],
    "GORDO": ["el-gordo", "gordo de la primitiva", "gordo-primitiva"],
    "EURO": ["euromillones", "euro-millones", "euromillon"],
}

# Rutas típicas (probables). El autodiscovery las validará primero:
STATIC_GUESSES = {
    "PRIMITIVA": ["/historico-primitiva/","/historico-de-resultados-primitiva/"],
    "BONOLOTO": ["/historico-bonoloto/","/historico-de-resultados-bonoloto/"],
    "GORDO": ["/historico-el-gordo-de-la-primitiva/","/historico-gordo-primitiva/"],
    "EURO": ["/historico-euromillones/","/historico-de-resultados-euromillones/"],
}

# Blindaje: rutas “semilla” del repo por si todo falla (no publicamos vacío)
SEED_LATEST_URL   = "https://raw.githubusercontent.com/gssecotrade/fran-ops/main/docs/api/seed_latest.json"
SEED_HISTORIC_URL = "https://raw.githubusercontent.com/gssecotrade/fran-ops/main/docs/api/seed_historic.json"


def http_get(url: str) -> requests.Response:
    r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r


def discover_candidates(game: str) -> List[str]:
    """Intenta encontrar la URL del histórico para el juego."""
    cands: List[str] = []

    # 1) Intentos estáticos
    for path in STATIC_GUESSES.get(game, []):
        cands.append(urljoin(BASE, path))

    # 2) sitemap(s)
    for sm in ("/sitemap_index.xml", "/sitemap.xml"):
        try:
            r = http_get(urljoin(BASE, sm))
            soup = BeautifulSoup(r.text, "xml")
            for loc in soup.select("loc"):
                u = loc.get_text(strip=True)
                if not u:
                    continue
                lu = u.lower()
                if "histor" in lu:  # historico / histórico / history
                    # filtrado por palabras clave del juego
                    if any(k in lu for k in KEYWORDS[game]):
                        cands.append(u)
        except Exception as e:
            logging.debug(f"[sitemap] {sm}: {e}")

    # 3) enlaces de homepage
    try:
        r = http_get(BASE + "/")
        home = BeautifulSoup(r.text, "html.parser")
        for a in home.select("a[href]"):
            href = a["href"]
            if not href:
                continue
            u = href if bool(urlparse(href).netloc) else urljoin(BASE, href)
            lu = u.lower()
            if "histor" in lu and any(k in lu for k in KEYWORDS[game]):
                cands.append(u)
    except Exception as e:
        logging.debug(f"[home] links: {e}")

    # ordenar y deduplicar preservando orden
    seen = set()
    dedup: List[str] = []
    for u in cands:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    return dedup


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


def parse_table_generic(html: str, game: str) -> List[Dict[str, Any]]:
    """
    Parser tolerante sobre la primera <table> “grande”.
    Cabecera esperada (nombres flexibles):
      FECHA | N1 | N2 | N3 | N4 | N5 | N6 | (Compl/Clave) | (R) | (E1) | (E2)
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table")
    if not tables:
        return []

    # Elegimos la tabla con más filas
    table = max(tables, key=lambda t: len(t.select("tr")))
    rows = table.select("tr")
    if len(rows) < 2:
        return []

    header = [th.get_text(strip=True).upper() for th in rows[0].find_all(["th","td"])]
    if not header:
        return []

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

    out: List[Dict[str, Any]] = []
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
        stars = []
        if e1 is not None: stars.append(e1)
        if e2 is not None: stars.append(e2)
        if stars: draw["estrellas"] = stars

        out.append(draw)

    return out


def fetch_game(game: str) -> List[Dict[str, Any]]:
    errors = []
    for u in discover_candidates(game):
        try:
            logging.info(f"[fetch] {game} -> {u}")
            r = http_get(u)
            data = parse_table_generic(r.text, game)
            if data:
                logging.info(f"[ok] {game}: {len(data)} sorteos")
                return data
            errors.append(f"parser vacío en {u}")
        except Exception as e:
            errors.append(f"{u}: {e}")
            time.sleep(1)

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
    **Blindaje de producción**: solo guardamos si hay resultados.
    Si no, NO tocamos el fichero (evitamos publicar JSON vacío).
    """
    if not payload.get("results"):
        logging.warning(f"[guard] No hay resultados; NO se sobreescribe {path}")
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logging.info(f"[done] Escrito {len(payload['results'])} sorteos en {path}")
