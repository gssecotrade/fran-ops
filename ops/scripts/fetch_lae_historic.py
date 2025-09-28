# OPS/scripts/fetch_lae_historic.py
# -*- coding: utf-8 -*-
"""
Genera docs/api/lae_historico.json con históricos por juego.
- Sólo publica sorteos que superan validaciones estrictas (fechas, rangos, conteos).
- Filtra fechas futuras (nada > hoy), elimina duplicados y ordena.
- Si un juego no alcanza minimo de sorteos válidos (MIN_OK_PER_GAME), no se publica ese juego.
- No inventa datos: si el origen no tiene info fiable, el juego queda fuera y se reporta en 'errors'.

Requisitos: requests, beautifulsoup4, lxml (el workflow ya los instala).
"""

import sys
import json
import time
import math
import traceback
from datetime import datetime, date
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup

# ---------- Configuración de origen (URLs oficiales de cada juego) ----------
# Nota: Son páginas públicas de LAE con listados de sorteos. El HTML puede cambiar.
# Este scraper es defensivo: si cambia la estructura, aborta sin publicar basura.
GAMES_URLS = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

# ---------- Parámetros de robustez ----------
HEADERS = {
    "User-Agent": "fran-ops-historic/1.0 (+github actions)"
}
TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN = 0.7

# Para evitar publicar historicos “casi vacíos” si un día la web no carga:
MIN_OK_PER_GAME = 50  # mínimo de sorteos válidos para publicar un juego

# ---------- Reglas de negocio por juego ----------
def rules_for_game(game: str) -> Dict[str, Any]:
    if game in ("PRIMITIVA", "BONOLOTO"):
        return {
            "count": 6,
            "range_min": 1, "range_max": 49,
            "allow_compl": True,   # complementario 1..49
            "allow_rein": True,    # reintegro 0..9
            "stars": None,
        }
    if game == "GORDO":
        # El Gordo: 5 números (1..54) + “clave/reintegro” (0..9)
        return {
            "count": 5,
            "range_min": 1, "range_max": 54,
            "allow_compl": False,
            "allow_rein": True,    # clave/reintegro 0..9
            "stars": None,
        }
    if game == "EURO":
        # Euromillones: 5 números 1..50 + 2 estrellas 1..12
        return {
            "count": 5,
            "range_min": 1, "range_max": 50,
            "allow_compl": False,
            "allow_rein": False,
            "stars": {"count": 2, "min": 1, "max": 12},
        }
    raise ValueError(f"Juego no soportado: {game}")

# ---------- Utilidades ----------
def http_get(url: str) -> str:
    last = None
    for i in range(RETRIES):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(SLEEP_BETWEEN * (i + 1))
    raise RuntimeError(f"Fallo GET {url}: {last}")

def parse_date(s: str) -> date:
    """
    Intenta parsear fechas típicas de LAE (dd/mm/yyyy, dd-mm-yyyy, yyyy-mm-dd).
    Devuelve objeto date o lanza ValueError.
    """
    s = (s or "").strip()
    fmts = ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d")
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except Exception:
            pass
    # algunos listados muestran 'jueves, 12 de septiembre de 2024' → intenta simplificar:
    try:
        import re
        s2 = s.lower()
        s2 = re.sub(r"de\s+", "", s2)
        meses = {
            "enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06",
            "julio":"07","agosto":"08","septiembre":"09","setiembre":"09","octubre":"10",
            "noviembre":"11","diciembre":"12"
        }
        for m_es, m_num in meses.items():
            s2 = s2.replace(m_es, m_num)
        # quita nombre del día si viene
        s2 = re.sub(r"^(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo),\s*", "", s2)
        # ahora debería quedar p.ej '12 09 2024'
        s2 = re.sub(r"[^\d ]", " ", s2)
        s2 = re.sub(r"\s+", " ", s2).strip()
        parts = s2.split()
        if len(parts) == 3:
            d, m, y = parts
            return date(int(y), int(m), int(d))
    except Exception:
        pass
    raise ValueError(f"Fecha no reconocida: '{s}'")

def ints_in_row(row) -> List[int]:
    out = []
    for td in row.find_all("td"):
        txt = td.get_text(" ", strip=True)
        for tok in txt.replace(",", " ").replace("·", " ").split():
            if tok.isdigit():
                out.append(int(tok))
    return out

def clean_draws(game: str, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Valida y normaliza sorteos por juego:
    - fechas válidas (>= 1990-01-01 y <= hoy)
    - conteos y rangos correctos por juego
    - unicidad por (game, date)
    - ordena por fecha ascendente
    """
    today = date.today()
    dmin = date(1990, 1, 1)
    rules = rules_for_game(game)
    seen = set()
    valid = []

    for r in raw_rows:
        try:
            d = parse_date(r["date"])
            if not (dmin <= d <= today):
                # filtra 2026, etc.
                continue

            nums: List[int] = list(map(int, r.get("numbers") or []))
            nums = [n for n in nums if rules["range_min"] <= n <= rules["range_max"]]
            # unicidad dentro del sorteo
            nums = sorted(set(nums))
            if len(nums) != rules["count"]:
                continue

            draw = {
                "game": game,
                "date": d.isoformat(),
                "numbers": nums,
            }

            # complementario / reintegro / estrellas según juego
            if rules["allow_compl"]:
                c = r.get("complementario")
                if c is not None and isinstance(c, int) and rules["range_min"] <= c <= rules["range_max"]:
                    draw["complementario"] = c

            if rules["allow_rein"]:
                rein = r.get("reintegro")
                if rein is not None and isinstance(rein, int) and 0 <= rein <= 9:
                    draw["reintegro"] = rein

            if rules["stars"]:
                stars = r.get("stars") or r.get("estrellas") or []
                stars = [int(x) for x in stars if rules["stars"]["min"] <= int(x) <= rules["stars"]["max"]]
                stars = sorted(set(stars))
                if len(stars) != rules["stars"]["count"]:
                    continue
                draw["estrellas"] = stars

            key = (game, draw["date"])
            if key in seen:
                continue
            seen.add(key)
            valid.append(draw)

        except Exception:
            # si algo no cuadra, descartamos esa fila
            continue

    # ordena por fecha asc
    valid.sort(key=lambda x: x["date"])
    return valid

# ---------- Scrapers básicos por juego ----------
def scrape_generic_table(html: str) -> List[Dict[str, Any]]:
    """
    Intenta extraer filas de una tabla de sorteos.
    Devuelve estructuras crudas con los campos que logremos inferir:
    - "date" (string)
    - "numbers" (lista de ints detectados en la fila)
    - y, si logramos, complementario / reintegro / estrellas
    """
    out = []
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if not tables:
        return out

    # Heurística: usa la primera tabla “grande”
    table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = table.find_all("tr")
    if len(rows) <= 1:
        return out

    # Suponemos cabecera en row[0]
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue

        # Fecha en la primera celda con pinta de fecha
        date_txt = None
        for cand in tds[:3]:
            txt = cand.get_text(" ", strip=True)
            if any(ch.isdigit() for ch in txt):
                # lo intentaremos parsear luego
                date_txt = txt
                break
        if not date_txt:
            continue

        ints = ints_in_row(tr)
        raw = {
            "date": date_txt,
            "numbers": [],
        }

        # Heurística por cantidad de enteros detectados:
        # - Si hay 6–8 → probablemente 6 números + compl/rein.
        # - Si hay 7 → primitiva/bonoloto 6 + 1 extra.
        # - Si hay 7–9 y “estrella” o similar en la fila → puede ser EURO.
        # Como no queremos inventar, mapeamos conservadoramente:
        if len(ints) >= 5:
            raw["numbers"] = ints[:6] if len(ints) >= 6 else ints[:5]
            rest = ints[6:] if len(ints) > 6 else []

            # intentar deducir extras comunes
            # complementario suele estar en 1..49, reintegro 0..9
            for val in rest:
                if 0 <= val <= 9 and "reintegro" not in raw:
                    raw["reintegro"] = val
                elif 1 <= val <= 49 and "complementario" not in raw:
                    raw["complementario"] = val

            # estrellas (para EURO): si detectamos más ints pequeños (1..12), guárdalos en 'estrellas'
            stars = [v for v in rest if 1 <= v <= 12]
            if stars:
                raw["estrellas"] = stars[:2]

        out.append(raw)

    return out

def fetch_game(game: str, url: str) -> List[Dict[str, Any]]:
    print(f"[fetch] {game} -> {url}")
    html = http_get(url)
    raw_rows = scrape_generic_table(html)
    cleaned = clean_draws(game, raw_rows)
    return cleaned

# ---------- main ----------
def main(outfile: str):
    all_results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for game, url in GAMES_URLS.items():
        try:
            draws = fetch_game(game, url)
            if len(draws) < MIN_OK_PER_GAME:
                errors.append(f"{game}: too_few_valid_draws ({len(draws)})")
                print(f"[warn] {game}: sólo {len(draws)} sorteos válidos → no se publican")
                continue
            all_results.extend(draws)
            print(f"[ok] {game}: {len(draws)} sorteos válidos")
        except Exception as e:
            msg = f"{game}: {e.__class__.__name__}: {e}"
            errors.append(msg)
            traceback.print_exc()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": all_results,
        "errors": errors,
    }

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    total = sum(1 for _ in all_results)
    print(f"[done] Escrito {total} sorteos válidos en {outfile}")
    if errors:
        print("[notes] " + " | ".join(errors))

if __name__ == "__main__":
    outfile = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_historico.json"
    main(outfile)
