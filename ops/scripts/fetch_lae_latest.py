# ops/scripts/fetch_lae_latest.py
# Construye docs/api/lae_latest.json a partir de docs/api/lae_historico.json

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any, List

from fetch_lae_common import today_utc, write_json, latest_by_game

HIST = Path("docs/api/lae_historico.json")
OUT  = Path("docs/api/lae_latest.json")
OUT.parent.mkdir(parents=True, exist_ok=True)


def load_hist() -> Dict[str, Any]:
    if HIST.exists():
        with open(HIST, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"generated_at": today_utc(), "results": [], "errors": ["no historic file"]}


def main():
    hist = load_hist()
    all_results: List[Dict[str, Any]] = hist.get("results", [])
    payload = {
        "generated_at": today_utc(),
        "results": latest_by_game(all_results),
        "errors": [],
    }
    write_json(str(OUT), payload)


if __name__ == "__main__":
    main()
