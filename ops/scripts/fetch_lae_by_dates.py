# ops/scripts/fetch_lae_by_dates.py
# Captura robusta por fechas (HTML SEO) de LAE, respetando días reales de sorteo
# con tolerancia d-1/d/d+1 para cambios puntuales. Rango: 2020..hoy.

import os, re, json, time, random
from datetime import date, timedelta, datetime
from typing import List, Dict, Any, Optional
import requests

OUT_DIR = os.path.join("docs", "api")

# Nuestro modelo usa últimos 5 años
START_YEAR = 2020
END_YEAR   = date.today().year

# Días reales de sorteo (0=Lunes ... 6=Domingo)
WEEKDAYS = {
    "PRIMITIVA": {0, 3, 5},             # L, J, S
    "BONOLOTO":  {0, 1, 2, 3, 4, 5},     # L a S
    "EURO":      {1, 4},                 # M, V
    "GORDO":     {6},                    # D
}

# Páginas públicas de resultados (evitan WAF del API JSON)
GAMES: Dict[str, Dict[str, Any]] = {
    "PRIMITIVA": {
        "url_patterns": [
            "https://www.loteriasyapuestas.es/es/la-primitiva/resultados/{YYYY}-{MM}-{DD}",
            "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos/{YYYY}-{MM}-{DD}",
        ],
        "main_count": 6, "has_complementario": True, "has_reintegro": True,
    },
    "BONOLOTO": {
        "url_patterns": [
            "https://www.loteriasyapuestas.es/es/bonoloto/resultados/{YYYY}-{MM}-{DD}",
            "https://www.loteriasyapuestas.es/es/bonoloto/sorteos/{YYYY}-{MM}-{DD}",
        ],
        "main_count": 6, "has_complementario": True, "has_reintegro": True,
    },
    "EURO": {
        "url_patterns": [
            "https://www.loteriasyapuestas.es/es/euromillones/resultados/{YYYY}-{MM}-{DD}",
            "https://www.loteriasyapuestas.es/es/euromillones/sorteos/{YYYY}-{MM}-{DD}",
        ],
        "main_count": 5, "stars_count": 2,
    },
    "GORDO": {
        "url_patterns": [
            "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/resultados/{YYYY}-{MM}-{DD}",
            "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos/{YYYY}-{MM}-{DD}",
        ],
        "main_count": 6, "has_clave": True,
    },
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
})
TIMEOUT = 20

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def daterange(start_y: int, end_y: int):
    d = date(start_y, 1, 1)
    end = date(end_y, 12, 31)
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one

def http_get(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        if r.status_code == 200 and "<html" in r.text.lower():
            return r.text
        return None
    except Exception:
        return None

# ---------- Parsers robustos (múltiples maquetaciones) ----------
def pick_ints_by_class(html: str, class_keyword: str) -> List[int]:
    nums: List[int] = []
    for m in re.finditer(rf'class="[^"]*{class_keyword}[^"]*"[^>]*>\s*(\d{{1,2}})\s*<', html, re.I):
        try: nums.append(int(m.group(1)))
        except: pass
    return nums

def pick_main_numbers(html: str, max_count: int) -> List[int]:
    for key in ("bola", "ball", "numero"):
        nums = pick_ints_by_class(html, key)
        if nums:
            return nums[:max_count]
    nums: List[int] = []
    for m in re.finditer(r'data-ball[^>]*>\s*(\d{1,2})\s*<', html, re.I):
        try: nums.append(int(m.group(1)))
        except: pass
    if nums: return nums[:max_count]
    block = re.search(r'Combinaci[oó]n.*?(<[^>]+>.*?</[^>]+>)', html, re.I | re.S)
    if block:
        tmp = re.findall(r'>(\d{1,2})<', block.group(1))
        try: return list(map(int, tmp))[:max_count]
        except: return []
    return []

def pick_after_label(html: str, label: str, max_gap: int = 40) -> Optional[int]:
    m = re.search(label + rf'[^0-9]{{0,{max_gap}}}(\d{{1,2}})', html, re.I)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

def pick_stars(html: str, expect: int = 2) -> List[int]:
    stars = pick_ints_by_class(html, "estrella")
    if len(stars) >= expect:
        return stars[:expect]
    after = re.search(r'Estrellas?(.*?)(</section>|</div>)', html, re.I | re.S)
    if after:
        tmp = re.findall(r'>(\d{1,2})<', after.group(1))
        got: List[int] = []
        for t in tmp:
            try: got.append(int(t))
            except: pass
        if len(got) >= expect:
            return got[:expect]
    return []

def parse_draw(game: str, html: str, ymd: str, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    main = pick_main_numbers(html, cfg["main_count"])
    if not main:
        return None
    out: Dict[str, Any] = {"game": game, "date": ymd, "numbers": main}

    if cfg.get("has_complementario"):
        c = pick_after_label(html, r'Complementario')
        if c is not None: out["complementario"] = c

    if cfg.get("has_reintegro"):
        r = pick_after_label(html, r'Reintegro')
        if r is not None: out["reintegro"] = r

    if "stars_count" in cfg:
        stars = pick_stars(html, cfg["stars_count"])
        if stars: out["estrellas"] = stars

    if cfg.get("has_clave"):
        k = pick_after_label(html, r'Clave', max_gap=20)
        if k is not None: out["clave"] = k

    return out

def fetch_one_of_patterns(cfg: Dict[str, Any], d: date) -> Optional[str]:
    for pattern in cfg["url_patterns"]:
        url = pattern.format(YYYY=d.year, MM=f"{d.month:02d}", DD=f"{d.day:02d}")
        html = http_get(url)
        if html:
            return html
    return None

def fetch_with_neighbors(game: str, cfg: Dict[str, Any], d: date) -> Optional[Dict[str, Any]]:
    # intenta d, d-1, d+1
    for off in (0, -1, +1):
        dd = d + timedelta(days=off)
        html = fetch_one_of_patterns(cfg, dd)
        if not html:
            continue
        ymd = dd.strftime("%Y-%m-%d")
        draw = parse_draw(game, html, ymd, cfg)
        if draw:
            return draw
    return None

def fetch_game(game: str, cfg: Dict[str, Any], start_y: int, end_y: int) -> List[Dict[str, Any]]:
    allowed = WEEKDAYS.get(game, set())
    results_by_date: Dict[str, Dict[str, Any]] = {}
    print(f"[run] {game} => días de sorteo {sorted(allowed)} | rango {start_y}..{end_y}")

    for d in daterange(start_y, end_y):
        if d.weekday() not in allowed:
            continue
        got = fetch_with_neighbors(game, cfg, d)
        if got:
            results_by_date[got["date"]] = got
        time.sleep(0.035 + random.uniform(0, 0.035))  # suave anti-WAF

    arr = sorted(results_by_date.values(), key=lambda x: x["date"])
    print(f"[sum] {game} -> {len(arr)} sorteos")
    return arr

def latest_by_game(draws_by_game: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for g, arr in draws_by_game.items():
        best = None; best_dt = None
        for d in arr:
            try:
                dt = datetime.strptime(d["date"], "%Y-%m-%d")
            except:
                continue
            if best_dt is None or dt > best_dt:
                best_dt, best = dt, d
        if best:
            out.append(best)
    return out

def main():
    ensure_dir(OUT_DIR)
    print(f"=== LAE · HISTÓRICO por fechas (días reales con tolerancia) · {START_YEAR}..{END_YEAR} ===")
    all_by_game: Dict[str, List[Dict[str, Any]]] = {}

    for game, cfg in GAMES.items():
        all_by_game[game] = fetch_game(game, cfg, START_YEAR, END_YEAR)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_by_game.values(), []),
        "by_game_counts": {k: len(v) for k, v in all_by_game.items()},
        "meta": {"from_year": START_YEAR, "to_year": END_YEAR, "mode": "by_dates_html_days+neighbors"}
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    for g, arr in all_by_game.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": payload["generated_at"], "results": arr}, f, ensure_ascii=False, indent=2)

    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": payload["generated_at"], "results": latest_by_game(all_by_game)}, f, ensure_ascii=False, indent=2)

    print("by_game_counts:", payload["by_game_counts"])
    print("=== DONE ===")

if __name__ == "__main__":
    main()
