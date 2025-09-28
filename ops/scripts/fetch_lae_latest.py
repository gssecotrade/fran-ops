#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime

# Asegura import local
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from fetch_lae_common import _session, _get_html, scrape_lotoideas_table, write_json

SOURCES = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva",
    "BONOLOTO":  "https://www.lotoideas.com/historico-bonoloto",
    "GORDO":     "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva",
    "EURO":      "https://www.lotoideas.com/historico-euromillones",
}

def main(outfile: str):
    s = _session()
    results = []
    errors = []

    for game, url in SOURCES.items():
        try:
            html = _get_html(s, url)
            rows = scrape_lotoideas_table(html, game)
            if rows:
                results.append(rows[0])  # la más reciente suele ser la primera
                print(f"[ok] {game}: último={rows[0].get('date')}")
            else:
                errors.append(f"{game}: sin filas")
        except Exception as e:
            errors.append(f"{game}: {e}")

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": results,
        "errors": errors,
    }
    write_json(outfile, payload)
    print(f"[done] latest -> {outfile} | juegos_ok={len(results)} | errores={len(errors)}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join("docs", "api", "lae_latest.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    main(out)
