#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, re, json, time, datetime as dt
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "LAE-Bot/1.0 (+github actions; data for analysis)"
}
TIMEOUT = 30

# Fuentes oficiales (HTML) – se intenta primero:
OFFICIAL = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

# Fuentes espejo (tablas públicas muy limpias) – fallback:
MIRRORS = {
    "PRIMITIVA": "https://www.lotoideas.com/historico/resultados-primitiva.php",
    "BONOLOTO":  "https://www.lotoideas.com/historico/resultados-bonoloto.php",
    "GORDO":     "https://www.lotoideas.com/historico/resultados-el-gordo-primitiva.php",
    "EURO":      "https://www.lotoideas.com/historico/resultados-euromillones.php",
}

DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")

def to_iso(date_str):
    """Convierte dd/mm/yyyy o dd-mm-yyyy -> ISO yyyy-mm-dd."""
    m = DATE_RE.search(date_str)
    if not m: 
        return None
    d, m_, y = map(int, m.groups())
    try:
        return dt.date(y, m_, d).isoformat()
    except ValueError:
        return None

def get(url):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def parse_official_latest(game, html):
    """
    Heurística: en las páginas oficiales suele aparecer un bloque de
    'último sorteo' con bolas <li>, o tablas. Extraemos 1..3 más recientes.
    """
    soup = BeautifulSoup(html, "lxml")
    draws = []

    # 1) busca bloques que contengan 6 bolas (N1..N6) + complementario/reintegro/estrellas
    #    (los selectores varían a menudo; mantenemos heurística flexible)
    for block in soup.find_all(True):
        balls = [int(x.get_text(strip=True)) for x in block.find_all('li')
                 if x.get_text(strip=True).isdigit()]
        if len(balls) >= 5:
            # busca fecha en el mismo bloque (o padres)
            text = " ".join(block.get_text(" ", strip=True).split())
            iso = to_iso(text)
            if iso:
                draw = {"game": game, "date": iso, "numbers": balls[:6]}
                # complementario/reintegro/estrellas (heurístico)
                if "reintegro" in text.lower():
                    # saca reintegro si solo hay 1 dígito aislado
                    rein = re.findall(r"reintegro[^0-9]*(\d{1,2})", text.lower())
                    if rein: draw["reintegro"] = int(rein[0])
                if "complementario" in text.lower():
                    comp = re.findall(r"complementario[^0-9]*(\d{1,2})", text.lower())
                    if comp: draw["complementario"] = int(comp[0])
                if "estrella" in text.lower():
                    estrella = re.findall(r"estrella[^0-9]*(\d{1,2})", text.lower())
                    if len(estrella) >= 2:
                        draw["estrellas"] = [int(estrella[0]), int(estrella[1])]
                draws.append(draw)
                if len(draws) >= 3:
                    break

    return dedup_and_sort(draws)

def parse_mirror_latest(game, html):
    """De tablas espejo (lotoideas). Tomamos 1..3 filas más recientes."""
    soup = BeautifulSoup(html, "lxml")
    t = soup.find("table")
    if not t:
        return []
    rows = t.find_all("tr")
    out = []
    for tr in rows[1:5]:  # 4 filas bastan para cubrir 1..3 últimas
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if not cols: 
            continue
        # suele ser: FECHA, 6 números, comp, reintegro/clave/etc
        iso = to_iso(cols[0])
        if not iso: 
            continue
        nums = [int(x) for x in cols[1:7] if x.isdigit()]
        draw = {"game": game, "date": iso, "numbers": nums[:6]}
        # complementario / reintegro / estrellas / clave según juego:
        if game in ("PRIMITIVA","BONOLOTO"):
            if len(cols) >= 9 and cols[7].isdigit():
                draw["complementario"] = int(cols[7])
            if len(cols) >= 10 and cols[8].isdigit():
                draw["reintegro"] = int(cols[8])
        elif game == "GORDO":
            # 5 números + clave
            if len(cols) >= 7 and cols[6].isdigit():
                draw["clave"] = int(cols[6])
        elif game == "EURO":
            # 5 números + 2 estrellas
            stars = [int(x) for x in cols[6:8] if x.isdigit()]
            if len(stars) == 2:
                draw["estrellas"] = stars
        out.append(draw)
        if len(out) >= 3:
            break
    return dedup_and_sort(out)

def dedup_and_sort(draws):
    bykey = {}
    for d in draws:
        if not d.get("date"): 
            continue
        k = (d["game"], d["date"])
        if k not in bykey:
            bykey[k] = d
    out = list(bykey.values())
    out.sort(key=lambda x: (x["game"], x["date"]), reverse=True)
    return out

def fetch_latest():
    results = []
    for g, url in OFFICIAL.items():
        try:
            html = get(url)
            ds = parse_official_latest(g, html)
            if not ds:
                # fallback espejo
                html = get(MIRRORS[g])
                ds = parse_mirror_latest(g, html)
            results.extend(ds)
        except Exception as e:
            print(f"[error] {g}: {e}")
            # intenta al menos espejo si falló oficial
            try:
                html = get(MIRRORS[g])
                results.extend(parse_mirror_latest(g, html))
            except Exception as e2:
                print(f"[error] {g} (mirror): {e2}")
    return dedup_and_sort(results)

def main(outfile):
    res = fetch_latest()
    payload = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "results": res,
        "errors": []
    }
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[done] latest -> {outfile} ({len(res)} draws)")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_latest.json"
    main(out)
