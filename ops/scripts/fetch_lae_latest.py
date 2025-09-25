#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga el último sorteo de PRIMITIVA, BONOLOTO, GORDO y EUROMILLONES
usando el proxy-caché público r.jina.ai para evitar 403 de LAE.
Genera: dist/lae_latest.json
"""

import json, re, datetime as dt
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://r.jina.ai/http://www.loteriasyapuestas.es/"
PAGES = {
    "PRIMITIVA": "es/la-primitiva",
    "BONOLOTO":  "es/bonoloto",
    "GORDO":     "es/el-gordo-de-la-primitiva",
    "EURO":      "es/euromillones",
}
UA = {"User-Agent": "Mozilla/5.0 (FranOps/1.0)"}

RE_DATE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
RE_NUM  = re.compile(r"\b\d{1,2}\b")

def fetch_html(path: str) -> str:
    url = urljoin(BASE, path)
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.text

def _first_date(text: str) -> str | None:
    m = RE_DATE.search(text)
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    return dt.date(y, mth, d).isoformat()

def parse_6_plus_cr(text: str) -> tuple[list[int], int | None, int | None]:
    """Para PRIMITIVA / BONOLOTO / GORDO: 6 números 1..49 + complementario (1..49) + reintegro (0..9)."""
    nums = [int(x) for x in RE_NUM.findall(text)]
    # Heurística: primeros 6 en 1..49
    main = []
    comp = None
    rein = None
    for n in nums:
        if 1 <= n <= 49 and len(main) < 6 and n not in main:
            main.append(n)
    # Complementario: el siguiente 1..49
    for n in nums:
        if 1 <= n <= 49 and n not in main:
            comp = n
            break
    # Reintegro: busca la palabra y coge 0..9 cercana
    rein_m = re.search(r"Reintegro[^0-9]{0,10}(\d)", text, flags=re.I)
    if rein_m:
        rein = int(rein_m.group(1))
    # Fallback si no detecta reintegro
    if rein is None:
        for n in nums:
            if 0 <= n <= 9:
                rein = n
                break
    return main, comp, rein

def parse_euro(text: str) -> tuple[list[int], int | None, int | None]:
    """EUROMILLONES: 5 números 1..50 + 2 estrellas (1..12).
    Los guardamos como: main[5], comp=estrella1, rein=estrella2 (para encajar con la hoja)."""
    nums = [int(x) for x in RE_NUM.findall(text)]
    main = []
    stars = []
    for n in nums:
        if 1 <= n <= 50 and len(main) < 5 and n not in main:
            main.append(n)
    for n in nums:
        if 1 <= n <= 12 and len(stars) < 2:
            stars.append(n)
    comp = stars[0] if len(stars) >= 1 else None
    rein = stars[1] if len(stars) >= 2 else None
    return main, comp, rein

def parse_game(html: str, game: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    fecha = _first_date(text)
    if not fecha:
        return None
    if game == "EURO":
        main, comp, rein = parse_euro(text)
    else:
        main, comp, rein = parse_6_plus_cr(text)
    if len(main) < (5 if game == "EURO" else 6):
        return None
    return {
        "fecha": fecha,
        "n": main,
        "comp": comp,
        "rein": rein,
    }

def main():
    out = {"generated_at": dt.datetime.utcnow().isoformat() + "Z", "games": {}}
    for g, path in PAGES.items():
        try:
            html = fetch_html(path)
            parsed = parse_game(html, g)
            if parsed:
                out["games"][g] = parsed
        except Exception as e:
            out["games"][g] = {"error": repr(e)}
    # Salida
    import os, pathlib
    pathlib.Path("dist").mkdir(parents=True, exist_ok=True)
    with open("dist/lae_latest.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
