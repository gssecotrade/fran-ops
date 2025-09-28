# -*- coding: utf-8 -*-
import os
from fetch_lae_common import fetch_game, build_payload, safe_write_json

OUTFILE = os.environ.get("OUT_LATEST", "docs/api/lae_latest.json")

def main():
    results = []
    for game in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        data = fetch_game(game)
        # “latest” = cogemos el primero si la fuente está ordenada desc.
        if data:
            results.append(data[0])

    payload = build_payload(results)
    safe_write_json(OUTFILE, payload)

if __name__ == "__main__":
    main()
