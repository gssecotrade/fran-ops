#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, re, json, argparse, datetime as dt
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "LAE-Bot/1.0 (+github actions; data for analysis)"
}
TIMEOUT = 30

MIRRORS = {
    "PRIMITIVA": "https://www.lotoideas.com/historico/resultados-primitiva.php",
    "BONOLOTO":  "https://www.lotoideas.com/historico/resultados-bonoloto.php",
    "GORDO":     "https://www.lotoideas.com/historico/resultados-el-gordo-primitiva.php",
    "EURO":      "https://www.lotoideas.com/historico/resultados-euromillones.php",
}

DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")

def to_iso(date_str):
    m = DATE_RE.search(date_str)
    if not m: 
        return None
    d, m_, y = map(int, m.groups())
    try:
        return dt.date(y, m_, d).isoformat()
    except ValueError:
        return None

def get(url, params=None):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, params=params)
    r.raise_for_status()
    return r.text

def parse_table(game, html):
    soup = BeautifulSoup(html, "lxml")
    t = soup.find("table")
    if not t:
        return []
    rows = t.find_all("tr")
    out = []
    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["td","th"])]
        if not cols: continue
        iso = to_iso(cols[0])
        if not iso: 
            continue
        nums = [int(x) for x in cols[1:7] if x.isdigit()]
        draw = {"game": game, "date": iso, "numbers": nums[:6]}
        if game in ("PRIMITIVA","BONOLOTO"):
            if len(cols) >= 9 and cols[7].isdigit():
                draw["complementario"] = int(cols[7])
            if len(cols) >= 10 and cols[8].isdigit():
                draw["reintegro"] = int(cols[8])
        elif game == "GORDO":
            if len(cols) >= 7 and cols[6].isdigit():
                draw["clave"] = int(cols[6])
        elif game == "EURO":
            stars = [int(x) for x in cols[6:8] if x.isdigit()]
            if len(stars) == 2:
                draw["estrellas"] = stars
        out.append(draw)
    return out

def dedup_and_sort(draws):
    bykey = {}
    for d in draws:
        k = (d["game"], d["date"])
        bykey[k] = d
    out = list(bykey.values())
    out.sort(key=lambda x: (x["game"], x["date"]))
    return out

def fetch_historic(pages):
    """
    Algunas webs paginan con ?pagina=2,3... Otras cargan todo en una sola tabla.
    Este scraper mira primero si existe paginación simple (?page o ?p o ?pagina).
    Si no existe, lee la página base completa.
    """
    results = []
    for game, base in MIRRORS.items():
        got_any = False
        # 1) intenta paginación conocida
        for param in ("page","pagina","p"):
            try:
                for i in range(1, pages+1):
                    html = get(base, params={param: i})
                    ds = parse_table(game, html)
                    if not ds:
                        break
                    results.extend(ds)
                    got_any = True
            except Exception:
                pass
            if got_any:
                break
        # 2) si no hubo paginación o falló, una pasada “flat”
        if not got_any:
            try:
                html = get(base)
                results.extend(parse_table(game, html))
            except Exception as e:
                print(f"[error] {game}: {e}")
    return dedup_and_sort(results)

def main(outfile, pages):
    res = fetch_historic(pages)
    payload = {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "results": res,
        "errors": []
    }
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[done] historic -> {outfile} ({len(res)} draws)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("outfile", nargs="?", default="docs/api/lae_historico.json")
    ap.add_argument("--pages", type=int, default=12,
                    help="páginas a recorrer si hay paginación (por juego)")
    args = ap.parse_args()
    main(args.outfile, args.pages)
