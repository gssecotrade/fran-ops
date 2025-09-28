#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from fetch_lae_common import scrape_all, build_payload_historic, write_json, API_DIR

MAX_PAGES = 12  # ajusta fondo histórico

def main():
    data = scrape_all(max_pages=MAX_PAGES)
    payload = build_payload_historic(data)
    write_json(API_DIR / "lae_historico.json", payload)
    print(f"[done] historico listo con {len(payload.get('results',[]))} sorteos")

if __name__ == "__main__":
    main()
