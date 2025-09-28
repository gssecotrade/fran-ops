# OPS/scripts/fetch_lae_historic.py
# Fuente: https://www.lotoideas.com (páginas de "Histórico de Resultados")
# Salida: docs/api/lae_historico.json con estructura:
# {
#   "generated_at": "...Z",
#   "results": [ {game, date(dd/MM/yyyy), numbers[...], complementario?, reintegro?, estrellas?} ],
#   "errors": [...]
# }

import sys, json, time, re
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9"
})

# Rutas candidatas por juego en lotoideas (probadas)
CANDIDATE_URLS: Dict[str, List[str]] = {
    "BONOLOTO": [
        "https://www.lotoideas.com/historico-de-resultados-bonoloto",
        "https://www.lotoideas.com/historico-bonoloto",
    ],
    "PRIMITIVA": [
        "https://www.lotoideas.com/historico-de-resultados-primitiva",
        "https://www.lotoideas.com/historico-primitiva",
    ],
    "GORDO": [
        "https://www.lotoideas.com/historico-de-resultados-el-gordo-de-la-primitiva",
        "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva",
    ],
    "EURO": [
        "https://www.lotoideas.com/historico-de-resultados-euromillones",
        "https://www.lotoideas.com/historico-euromillones",
    ],
}

DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")  # dd/mm/yyyy
NUM_RE  = re.compile(r"^\d{1,2}$")

def ddmmyyyy(s: str) -> Optional[str]:
    s = s.strip()
    m = DATE_RE.search(s)
    if not m:
        return None
    d, mth, y = m.group(1), m.group(2), m.group(3)
    return f"{d}/{mth}/{y}"

def cell_numbers(cells: List[str]) -> List[int]:
    out = []
    for c in cells:
        c2 = c.strip().replace("·", "").replace(".", "")
        # Muchos sitios usan 01,02... -> int()
        if NUM_RE.match(c2):
            try:
                out.append(int(c2))
            except:
                pass
    return out

def fetch_table_rows(url: str, page: int) -> Tuple[List[List[str]], bool]:
    """
    Lee una página (si soporta paginación con ?page=N o /page/N).
    Retorna (filas_en_texto, hay_mas_paginas?)
    """
    # Probar variantes de paginación:
    candidates = [url]
    # ?page=N
    if page > 1:
        candidates.append(f"{url}?page={page}")
        # /page/N
        if not url.endswith("/"):
            candidates.append(f"{url}/page/{page}")
        else:
            candidates.append(f"{url}page/{page}")

    last_exc = None
    html = None
    for u in candidates:
        try:
            r = SESSION.get(u, timeout=30)
            # algunos sitios devuelven 200 con sínonimos; quedarnos con primera 200
            if r.status_code == 200 and ("<table" in r.text.lower()):
                html = r.text
                break
        except Exception as e:
            last_exc = e
    if html is None:
        if last_exc:
            raise last_exc
        raise RuntimeError(f"No se pudo obtener HTML válido para {url} (page {page})")

    soup = BeautifulSoup(html, "html.parser")

    # Buscar la tabla principal de resultados
    # Heurística: la primera tabla con cabecera que tenga la palabra 'FECHA'
    table = None
    for t in soup.find_all("table"):
        head = t.find("thead") or t.find("tr")
        if not head:
            continue
        text = t.get_text(" ", strip=True).upper()
        if "FECHA" in text and ("GANADORA" in text or "COMB." in text or "COMBINACIÓN" in text or "COMBINACION" in text):
            table = t
            break
    if not table:
        # fallback a primera tabla
        ts = soup.find_all("table")
        if ts:
            table = ts[0]
        else:
            return ([], False)

    rows = []
    for tr in table.find_all("tr"):
        cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        # saltar cabeceras
        if cols and cols[0].strip().upper() in ("FECHA", "FECHAS"):
            continue
        if cols:
            rows.append(cols)

    # heurística muy simple para "hay más páginas":
    hay_mas = False
    pager = soup.find(class_=re.compile("pagination|pager|paginacion", re.I)) or soup.find("nav", class_=re.compile("pagination", re.I))
    if pager and ("next" in pager.get_text(" ", strip=True).lower() or "siguiente" in pager.get_text(" ", strip=True).lower()):
        hay_mas = True

    return (rows, hay_mas)

def parse_row_for_game(game: str, cols: List[str]) -> Optional[Dict]:
    """
    Devuelve un draw normalizado o None.
    Para evitar depender de posiciones exactas, detectamos:
      - fecha en la primera o primeras columnas
      - números: todos los enteros de la fila
      - extras por juego: complementario/reintegro/estrellas/clave
    """
    if not cols:
        return None

    # Fecha (suele ir en la col 0)
    date = None
    for c in cols[:2]:
        d = ddmmyyyy(c)
        if d:
            date = d
            break
    if not date:
        return None

    nums_all = cell_numbers(cols[1:])  # ignorar la fecha
    if not nums_all:
        return None

    draw = {"game": game, "date": date}

    # Reglas por juego
    g = game.upper()
    if g in ("PRIMITIVA", "BONOLOTO"):
        # esperamos 6 + compl + reintegro
        # tomar siempre los 6 primeros como números
        if len(nums_all) < 6:
            return None
        draw["numbers"] = nums_all[:6]
        # extras si existen
        if len(nums_all) >= 7:
            draw["complementario"] = nums_all[6]
        # el reintegro suele venir como última cifra de la fila; si hay 8+ coger la última
        if len(nums_all) >= 8:
            draw["reintegro"] = nums_all[-1]

    elif g == "EURO":
        # 5 números + 2 estrellas
        if len(nums_all) < 7:
            return None
        draw["numbers"] = nums_all[:5]
        draw["estrellas"] = nums_all[5:7]

    elif g == "GORDO":
        # 5 números + clave (1)
        if len(nums_all) < 6:
            return None
        draw["numbers"] = nums_all[:5]
        draw["clave"] = nums_all[5]

    else:
        # default genérico: al menos 5
        if len(nums_all) < 5:
            return None
        draw["numbers"] = nums_all[:6]

    return draw

def scrape_game(game: str, urls: List[str], limit_pages: int = 120) -> Tuple[List[Dict], Optional[str]]:
    """
    Intenta múltiples urls candidatas.
    Pagina hasta que no haya más filas o llegue a limit_pages.
    """
    for base in urls:
        results = []
        last_rows = -1
        error = None
        for page in range(1, limit_pages + 1):
            try:
                rows, more = fetch_table_rows(base, page)
                if not rows:
                    if page == 1:
                        error = f"Sin filas en {base}"
                    break
                for r in rows:
                    draw = parse_row_for_game(game, r)
                    if draw:
                        results.append(draw)

                # corte si no crece
                if len(results) == last_rows:
                    break
                last_rows = len(results)

                if not more:
                    break

                # pequeño respiro
                time.sleep(0.3)

            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                break

        if results:
            return (results, None)
        # si esta base falló, probar la siguiente
    return ([], error or "No se pudo obtener datos")

def main(outfile: str):
    payload = {"generated_at": datetime.utcnow().isoformat() + "Z", "results": [], "errors": []}

    for game, urls in CANDIDATE_URLS.items():
        print(f"[fetch] {game}")
        results, err = scrape_game(game, urls)
        if err:
            print(f"[warn] {game}: {err}")
            payload["errors"].append(f"{game}: {err}")
        else:
            print(f"[ok] {game}: {len(results)} sorteos")
            payload["results"].extend(results)

    # Ordenar por fecha (asc) por consistencia (dd/MM/yyyy -> yyyymmdd)
    def key_date(d: Dict) -> str:
        try:
            dd, mm, yy = d["date"].split("/")
            return f"{yy}{mm}{dd}"
        except Exception:
            return d["date"]
    payload["results"].sort(key=key_date)

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[done] Escrito {len(payload['results'])} sorteos en {outfile}")

if __name__ == "__main__":
    outfile = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_historico.json"
    main(outfile)
