#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime

# Asegura que podamos importar fetch_lae_common.py desde esta misma carpeta
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

# Paginación máxima a intentar (ajústalo si hace falta)
MAX_PAGES = 50


def _iter_pages(base_url):
    """
    Genera URLs de paginado intentando distintos patrones comunes:
      /page/2  |  ?page=2  |  ?_paged=2
    La página 1 es siempre la base_url.
    """
    # Página 1
    yield base_url

    # /page/N
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}/page/{n}"

    # Por si el sitio usa querystring:
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}?page={n}"
    for n in range(2, MAX_PAGES + 1):
        yield f"{base_url}?_paged={n}"


def fetch_game(game_id, base_url):
    """
    Descarga y parsea el histórico de un juego recorriendo paginado
    hasta que no haya más filas válidas.
    """
    s = _session()
    resultados = []
    vistos = set()  # para evitar duplicados por distintas variantes de paginado

    for url in _iter_pages(base_url):
        if url in vistos:
            continue
        vistos.add(url)

        try:
            html = _get_html(s, url)
            filas = scrape_lotoideas_table(html, game_id)
            if not filas:
                # si en el primer patrón ya no hay filas, probamos los otros patrones
                continue
            resultados.extend(filas)
        except Exception:
            # si una variante de paginado falla, probamos la siguiente
            continue

        # Heurística de corte: si una página devuelve pocas filas y la
        # anterior ya devolvió algo, paramos para evitar loops largos.
        if len(filas) < 3:
            break

    return resultados


def main(outfile):
    all_results = []
    errors = []

    for game, url in SOURCES.items():
        try:
            data = fetch_game(game, url)
            all_results.extend(data)
            print(f"[ok] {game}: {len(data)} sorteos")
        except Exception as e:
            msg = f"{game}: {e}"
            print("[error]", msg)
            errors.append(msg)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": all_results,
        "errors": errors,
    }
    write_json(outfile, payload)
    print(f"[done] histórico -> {outfile} | sorteos={len(all_results)} | errores={len(errors)}")


if __name__ == "__main__":
    # por defecto escribe en docs/api/lae_historico.json
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join("docs", "api", "lae_historico.json")
    # asegúrate de que exista docs/api
    os.makedirs(os.path.dirname(out), exist_ok=True)
    main(out)
