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

def _max_pages() -> int:
    try:
        v = int(os.environ.get("LAE_MAX_PAGES", "50"))
        return max(1, min(200, v))
    except Exception:
        return 50

def _iter_pages(base_url, max_pages: int):
    yield base_url  # página 1
    for n in range(2, max_pages + 1):
        yield f"{base_url}/page/{n}"
    for n in range(2, max_pages + 1):
        yield f"{base_url}?page={n}"
    for n in range(2, max_pages + 1):
        yield f"{base_url}?_paged={n}"

def fetch_game(game_id: str, base_url: str):
    s = _session()
    out = []
    seen = set()
    mp = _max_pages()
    pages_seen = 0

    for url in _iter_pages(base_url, mp):
        if url in seen:
            continue
        seen.add(url)
        pages_seen += 1
        print(f"[{game_id}] fetching page {pages_seen}/{mp}: {url}", flush=True)
        try:
            html = _get_html(s, url)
            rows = scrape_lotoideas_table(html, game_id)
            print(f"[{game_id}] rows on page: {len(rows)}", flush=True)
            if rows:
                out.extend(rows)
            else:
                # si una página ya no trae filas, paramos ese patrón de paginado
                continue
        except Exception as e:
            print(f"[{game_id}] warn: {e}", flush=True)
            continue

        # heurística: si una página trae muy pocas filas, corta el recorrido
        if len(rows) < 3:
            break

    print(f"[{game_id}] total collected: {len(out)}", flush=True)
    return out

def main(outfile: str):
    results = []
    errors = []

    for game, url in SOURCES.items():
        try:
            data = fetch_game(game, url)
            results.extend(data)
            print(f"[ok] {game}: {len(data)} sorteos", flush=True)
        except Exception as e:
            msg = f"{game}: {e}"
            errors.append(msg)
            print("[error]", msg, flush=True)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": results,
        "errors": errors,
    }
    write_json(outfile, payload)
    print(f"[done] histórico -> {outfile} | sorteos={len(results)} | errores={len(errors)}", flush=True)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join("docs", "api", "lae_historico.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    main(out)
