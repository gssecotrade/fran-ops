# -*- coding: utf-8 -*-
import os
from fetch_lae_common import fetch_game, build_payload, safe_write_json

OUTFILE = os.environ.get("OUT_HIST", "docs/api/lae_historico.json")

def main():
    results = []
    for game in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        results.extend(fetch_game(game))

    # Orden descendente por fecha para consistencia
    results.sort(key=lambda x: x.get("date",""), reverse=True)

    payload = build_payload(results)
    safe_write_json(OUTFILE, payload)

if __name__ == "__main__":
    main()
