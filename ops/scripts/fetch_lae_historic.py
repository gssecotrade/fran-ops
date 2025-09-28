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

# Fuentes públicas (históricos)
SOURCES = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva",
    "BONOLOTO":  "https://www.lotoideas.com/historico-bonoloto",
    "GORDO":     "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva",
    "EURO":      "https://www.lotoideas.com/historico-euromillones",
}

MAX_PAGES = 50  # límite de paginado


def _iter_pages(base_url):
    yield base_url  # página 1
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}/page/{n}"
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}?page={n}"
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}?_paged={n}"


def fetch_game(game_id: str, base_url: str):
    s = _session()
    out = []
    seen = set()

    for url in _iter_pages(base_url):
        if url in seen:
            continue
        seen.add(url)
        try:
            html = _get_html(s, url)
            rows = scrape_lotoideas_table(html, game_id)
            if not rows:
                continue
            out.extend(rows)
        except Exception:
            continue

        if len(rows) < 3:
            break

    return out


def main(outfile: str):
    results = []
    errors = []

    for game, url in SOURCES.items():
        try:
            data = fetch_game(game, url)
            results.extend(data)
            print(f"[ok] {game}: {len(data)} sorteos")
        except Exception as e:
            msg = f"{game}: {e}"
            errors.append(msg)
            print("[error]", msg)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": results,
        "errors": errors,
    }
    write_json(outfile, payload)
    print(f"[done] histórico -> {outfile} | sorteos={len(results)} | errores={len(errors)}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join("docs", "api", "lae_historico.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    main(out)
