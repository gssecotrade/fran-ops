#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import random
from datetime import datetime
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup

# ======== HTTP helpers ========

def _session() -> requests.Session:
    s = requests.Session()
    # Cabeceras para evitar 403 y parecer navegador real
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    })
    return s


def _get_html(s: requests.Session, url: str, tries: int = 4, backoff: float = 0.7) -> str:
    last = None
    for i in range(tries):
        try:
            r = s.get(url, timeout=30)
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")
            if r.status_code == 403:
                # pequeño backoff aleatorio y reintento
                time.sleep(backoff * (i + 1) + random.random())
                last = f"HTTP 403 ({url})"
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = str(e)
            time.sleep(backoff * (i + 1))
    raise RuntimeError(f"Falló GET {url}: {last}")


# ======== Parsing helpers ========

DATE_RE = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})")

def _parse_date(cell_text: str) -> str:
    """
    Convierte texto tipo '26/09/2025' o '26-09-25' a 'YYYY-MM-DD'.
    Devuelve '' si no reconoce fecha.
    """
    m = DATE_RE.search(cell_text.strip())
    if not m:
        return ""
    d, mth, y = m.groups()
    d = int(d); mth = int(mth); y = int(y)
    if y < 100:  # normaliza '25' -> 2025 (asumimos siglo XXI para históricos recientes)
        y += 2000
    try:
        return datetime(y, mth, d).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _to_ints(vs: List[str]) -> List[int]:
    out = []
    for v in vs:
        v = v.strip()
        if not v:
            continue
        v = v.replace(".", "").replace(",", "")
        if v.isdigit():
            out.append(int(v))
    return out


def _normalize_row(game: str, tds: List[str]) -> Dict[str, Any]:
    """
    Normaliza una fila en formato común:
      {game, date, numbers[...], complementario?, reintegro?/estrellas?/clave?}
    Asumimos tabla con:
      PRIMITIVA/BONOLOTO: FECHA, 6 números, COMPLEMENTARIO, REINTEGRO
      EL GORDO: FECHA, 5 números, CLAVE
      EUROMILLONES: FECHA, 5 números, 2 ESTRELLAS
    """
    row = [x.strip() for x in tds]
    if not row or len(row[0]) < 4:
        return {}

    date_iso = _parse_date(row[0])
    if not date_iso:
        return {}

    # Extrae números crudos (celdas que tengan 1-2 dígitos)
    nums_raw = []
    extras_raw = []
    for cell in row[1:]:
        # separa por espacios si vienen "01 08 13 ..."
        parts = re.split(r"\s+", cell.strip())
        parts = [p for p in parts if p]
        ints = _to_ints(parts)
        # heurística simple: primeras 6/5 entradas son números, resto extras
        if len(nums_raw) < 6:
            nums_raw.extend(ints)
        else:
            extras_raw.extend(ints)

    # Ajustes por juego
    out = {"game": game, "date": date_iso}

    if game in ("PRIMITIVA", "BONOLOTO"):
        # 6 números + complementario + reintegro
        numbers = nums_raw[:6]
        out["numbers"] = numbers
        # intenta deducir complementario y reintegro
        comp, rein = "", ""
        if len(extras_raw) >= 1:
            comp = extras_raw[0]
        if len(extras_raw) >= 2:
            rein = extras_raw[1]
        out["complementario"] = comp if comp != "" else ""
        out["reintegro"] = rein if rein != "" else ""

    elif game == "GORDO":
        # 5 números + clave
        numbers = nums_raw[:5]
        out["numbers"] = numbers
        clave = extras_raw[0] if extras_raw else ""
        out["clave"] = clave

    elif game == "EURO":
        # 5 números + 2 estrellas
        numbers = nums_raw[:5]
        out["numbers"] = numbers
        estrellas = extras_raw[:2] if len(extras_raw) >= 2 else []
        out["estrellas"] = estrellas

    else:
        out["numbers"] = nums_raw[:6]

    return out


def scrape_lotoideas_table(html: str, game: str) -> List[Dict[str, Any]]:
    """
    Busca una tabla de resultados en la página y devuelve lista de dicts normalizados.
    Es robusto a celdas con múltiples números dentro.
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    results: List[Dict[str, Any]] = []

    for tbl in tables:
        body = tbl.find("tbody") or tbl
        rows = body.find_all("tr")
        if len(rows) < 3:
            continue

        for i, tr in enumerate(rows):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            # salta cabeceras
            if i == 0 and any(x.upper().startswith("FECHA") for x in tds):
                continue
            if len(tds) < 2:
                continue
            item = _normalize_row(game, tds)
            if item:
                results.append(item)

        # Si hemos recogido suficientes filas, no seguimos buscando más tablas
        if len(results) >= 3:
            break

    return results


# ======== I/O ========

def write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
