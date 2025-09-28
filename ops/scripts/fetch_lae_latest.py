#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from fetch_lae_common import scrape_all, build_payload_latest, write_json, API_DIR

def main():
    data = scrape_all(max_pages=1)
    payload = build_payload_latest(data)
    write_json(API_DIR / "lae_latest.json", payload)
    print("[done] latest listo")

if __name__ == "__main__":
    main()
