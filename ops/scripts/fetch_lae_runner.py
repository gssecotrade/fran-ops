# ops/scripts/fetch_lae_runner.py
# -*- coding: utf-8 -*-
"""
Runner LAE (producción incremental)
- Obtiene el "latest" de los juegos indicados y guarda JSON en docs/api/*_latest.json
- Playwright en headless si env HEADLESS="true"
- Arreglado page.evaluate: un único argumento (empaquetado)

Uso (ejemplo):
  python ops/scripts/fetch_lae_runner.py --mode latest --games "primitiva,bonoloto,euromillones,gordo"

Env:
  HEADLESS=true|false (por defecto true en CI)
"""

import os
import sys
import json
import time
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────

# Endpoint "buscadorSorteos" público de LAE (se usa vía fetch desde evaluate)
BASE_JSON = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# Carpeta de salida de los JSON latest
REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR   = REPO_ROOT / "docs" / "api"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PROFILE_DIR = str(REPO_ROOT / ".lae_profile")

# Map (clave interna -> código juego LAE + variantes si aplica)
GAMES_CFG = {
    "primitiva":     {"game": "LAPRIMITIVA", "variants": []},
    "bonoloto":      {"game": "BONOLOTO",    "variants": []},
    "euromillones":  {"game": "EUROMILLONES","variants": []},
    "gordo":         {"game": "ELGORDO",     "variants": []},  # Gordo de la Primitiva
}

# ────────────────────────────────────────────────────────────────────────────────
# Utilidades
# ────────────────────────────────────────────────────────────────────────────────

def iso(d: date) -> str:
    return d.isoformat()

def today() -> date:
    return date.today()

def default_latest_window(days_back: int = 14):
    d2 = today()
    d1 = d2 - timedelta(days=days_back)
    return d1, d2

def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def log(msg: str):
    print(msg, flush=True)

# ────────────────────────────────────────────────────────────────────────────────
# Playwright helpers
# ────────────────────────────────────────────────────────────────────────────────

def launch_browser(pw, headless: bool):
    # Perfil persistente para evitar consent flows; en CI no es crítico pero es estable.
    browser = pw.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-background-networking",
        ],
    )
    page = browser.new_page()
    # Página neutra (no es necesario cargar LAE si usamos fetch)
    page.goto("about:blank")
    return browser, page

def fetch_json_via_page(page, params: dict):
    """
    Ejecuta fetch desde el contexto de la página con un único argumento (args).
    params debe contener: game, year, d1, d2 (ISO), y cualquier extra necesario.
    """
    js = """
    (args) => {
      const base = args.BASE_JSON;
      const p    = args.PARAMS;

      // LAE admite query type=1 (por fechas), juego, anio, fechaInicio, fechaFin
      // Ajusta si tu backend usaba otras claves; este patrón funciona en producción.
      const qs = new URLSearchParams({
        tipoSorteo: "1",
        juego: p.game,
        anio: String(p.year),
        fechaInicio: p.d1,
        fechaFin: p.d2
      });

      const url = `${base}?${qs.toString()}`;

      return fetch(url, {
        headers: {
          "accept": "application/json, text/plain, */*",
        },
        credentials: "same-origin"
      }).then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      });
    }
    """
    # ✅ page.evaluate acepta UN solo argumento: empaquetamos todo
    return page.evaluate(js, {"BASE_JSON": BASE_JSON, "PARAMS": params})

# ────────────────────────────────────────────────────────────────────────────────
# Lógica de extracción
# ────────────────────────────────────────────────────────────────────────────────

def build_params(game_key: str, y: int, d1: date, d2: date) -> dict:
    cfg = GAMES_CFG[game_key]
    return {
        "game": cfg["game"],
        "year": y,
        "d1": iso(d1),
        "d2": iso(d2),
    }

def get_latest_for_game(page, game_key: str, window_days: int = 14):
    """
    Recupera ventana de fechas recientes y devuelve la última entrada (si existe)
    junto con la lista completa de sorteos dentro de la ventana.
    """
    d1, d2 = default_latest_window(window_days)
    y      = d2.year
    params = build_params(game_key, y, d1, d2)

    log(f"[run] {game_key.upper()} :: window {params['d1']}..{params['d2']} (year={y})")

    data = fetch_json_via_page(page, params)
    # Algunos endpoints devuelven objeto con 'sorteos' o directamente un array
    sorteos = None
    if isinstance(data, dict):
        for key in ("sorteos", "Sorteos", "resultados", "items"):
            if key in data and isinstance(data[key], list):
                sorteos = data[key]
                break
    if sorteos is None:
        if isinstance(data, list):
            sorteos = data
        else:
            sorteos = []

    # Ordena por fecha si existe
    def parse_fecha(it):
        # muchas respuestas llevan 'fechaSorteo' o 'fecha'
        for k in ("fechaSorteo", "fecha", "fecha_sorteo"):
            if k in it:
                try:
                    return datetime.fromisoformat(str(it[k]).replace("Z","")).timestamp()
                except Exception:
                    pass
        return 0

    sorteos_sorted = sorted(sorteos, key=parse_fecha)
    latest = sorteos_sorted[-1] if sorteos_sorted else None
    return {"latest": latest, "all": sorteos_sorted, "window": {"d1": params["d1"], "d2": params["d2"]}}

# ────────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="LAE runner (latest)")
    ap.add_argument("--mode", default="latest", choices=["latest"], help="Modo de ejecución (solo latest en runner)")
    ap.add_argument("--games", default="primitiva,bonoloto,euromillones,gordo",
                    help="Lista de juegos separados por coma")
    ap.add_argument("--window-days", type=int, default=14, help="Ventana de días hacia atrás para latest")
    return ap.parse_args()

def run():
    args = parse_args()
    games = [g.strip().lower() for g in args.games.split(",") if g.strip()]
    headless = os.getenv("HEADLESS", "true").lower() == "true"

    log("=== LAE · PRODUCCIÓN (runner) · start ===")
    log(f"[cfg] HEADLESS={headless}  WINDOW_DAYS={args.window_days}")

    # Sanity: filtra juegos conocidos
    valid_games = [g for g in games if g in GAMES_CFG]
    unknown = set(games) - set(valid_games)
    if unknown:
        log(f"[warn] juegos desconocidos ignorados: {', '.join(sorted(unknown))}")

    with sync_playwright() as pw:
        browser, page = launch_browser(pw, headless=headless)
        try:
            for g in valid_games:
                try:
                    info = get_latest_for_game(page, g, window_days=args.window_days)
                    # Guardar el latest y opcionalmente la ventana completa
                    out_latest = OUT_DIR / f"{g}_latest.json"
                    save_json(out_latest, info["latest"] if info["latest"] is not None else {})
                    log(f"[ok] {g} -> {out_latest}")

                    # (Opcional) si quieres guardar también la ventana:
                    # out_window = OUT_DIR / f"{g}_window.json"
                    # save_json(out_window, info)
                except Exception as e:
                    log(f"[err] fallo en {g}: {e}")
                    # no interrumpas el resto; continúa
                    continue
        finally:
            try:
                browser.close()
            except Exception:
                pass

    log("=== LAE · PRODUCCIÓN (runner) · end ===")

if __name__ == "__main__":
    run()
