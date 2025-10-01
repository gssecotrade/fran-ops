# ops/scripts/fetch_lae_runner.py
# -*- coding: utf-8 -*-
"""
Runner LAE (latest) sin navegador:
- Usa Playwright APIRequest para GET directo (sin page.evaluate ni X server).
- Guarda JSON en docs/api/<game>_latest.json
"""

import os
import json
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR   = REPO_ROOT / "docs" / "api"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_JSON = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

GAMES_CFG = {
    "primitiva":     {"game": "LAPRIMITIVA"},
    "bonoloto":      {"game": "BONOLOTO"},
    "euromillones":  {"game": "EUROMILLONES"},
    "gordo":         {"game": "ELGORDO"},  # El Gordo de la Primitiva
}

def iso(d: date) -> str:
    return d.isoformat()

def window_days(n=14):
    d2 = date.today()
    d1 = d2 - timedelta(days=n)
    return d1, d2

def build_url(game_code: str, y: int, d1: date, d2: date) -> str:
    # Consulta por fechas (tipoSorteo=1). Ajustable si tu endpoint usa otras claves.
    qs = (
        f"tipoSorteo=1&juego={game_code}"
        f"&anio={y}&fechaInicio={iso(d1)}&fechaFin={iso(d2)}"
    )
    return f"{BASE_JSON}?{qs}"

def parse_sorteos(data):
    if isinstance(data, dict):
        for k in ("sorteos", "Sorteos", "resultados", "items"):
            if k in data and isinstance(data[k], list):
                return data[k]
    if isinstance(data, list):
        return data
    return []

def sort_key(it):
    for k in ("fechaSorteo", "fecha", "fecha_sorteo"):
        if k in it:
            try:
                return datetime.fromisoformat(str(it[k]).replace("Z","")).timestamp()
            except Exception:
                pass
    return 0

def save_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj or {}, f, ensure_ascii=False, indent=2)

def run_latest(games: list, win_days: int):
    print("=== LAE · PRODUCCIÓN (HTTP) · start ===", flush=True)
    d1, d2 = window_days(win_days)
    y = d2.year

    with sync_playwright() as pw:
        req = pw.request.new_context()  # HTTP client sin navegador
        headers = {"accept": "application/json, text/plain, */*"}

        for g in games:
            if g not in GAMES_CFG:
                print(f"[warn] juego desconocido: {g}", flush=True)
                continue

            game_code = GAMES_CFG[g]["game"]
            url = build_url(game_code, y, d1, d2)
            print(f"[run] {g.upper()} :: {url}", flush=True)

            try:
                resp = req.get(url, headers=headers, timeout=30000)
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                data = resp.json()
                sorteos = parse_sorteos(data)
                sorteos.sort(key=sort_key)
                latest = sorteos[-1] if sorteos else {}

                out = OUT_DIR / f"{g}_latest.json"
                save_json(out, latest)
                print(f"[ok] {g} -> {out}", flush=True)

            except Exception as e:
                print(f"[err] {g}: {e}", flush=True)
                # sigue con los demás

    print("=== LAE · PRODUCCIÓN (HTTP) · end ===", flush=True)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="latest", choices=["latest"])
    ap.add_argument("--games", default="primitiva,bonoloto,euromillones,gordo")
    ap.add_argument("--window-days", type=int, default=14)
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    games = [g.strip().lower() for g in args.games.split(",") if g.strip()]
    run_latest(games, args.window_days)
