# -*- coding: utf-8 -*-
import os
from fetch_lae_common import fetch_game, build_payload, safe_write_json

OUTFILE = os.environ.get("OUT_LATEST", "docs/api/lae_latest.json")

def main():
    results = []
    for game in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        data = fetch_game(game)
        if data:
            # latest: escogemos el primero (las páginas suelen estar en orden descendente)
            data.sort(key=lambda x: x.get("date",""), reverse=True)
            results.append(data[0])

    payload = build_payload(results)
    safe_write_json(OUTFILE, payload)

if __name__ == "__main__":
    main()
