# ops/scripts/fetch_lae_runner.py
# Scraper persistente LAE (Playwright) - runner propio
# - Usa perfil de navegador persistente (cookies/sesión/fingerprint)
# - Ritmo humano y backoff
# - Soporta histórico por años o modo incremental N días
# - Salida en docs/api/lae_historico.json y lae_latest.json (compatibles)

import os, sys, json, time, random
from datetime import datetime, date, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ------------ Configurable por ENV ------------
OUT_DIR = Path("docs/api")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Perfil persistente (no lo borres entre ejecuciones)
USER_DATA_DIR = Path(os.environ.get("LAE_PROFILE", ".lae_profile"))
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADLESS = os.environ.get("LAE_HEADLESS", "0") == "1"  # por defecto headful (0)
START_YEAR = int(os.environ.get("LAE_START_YEAR", "2020"))
END_YEAR   = int(os.environ.get("LAE_END_YEAR", str(date.today().year)))
# Incremental por días recientes (si LAE_DAYS está definido, ignora los años)
INCR_DAYS  = int(os.environ.get("LAE_DAYS", "0"))

# Ritmo humano
BASE_SLEEP = float(os.environ.get("LAE_BASE_SLEEP", "0.8"))  # segundos
JITTER     = float(os.environ.get("LAE_JITTER", "0.6"))

# Juegos y variantes de game_id que LAE espera en su buscador
GAMES = {
    "PRIMITIVA": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    "BONOLOTO":  ["BONOLOTO"],
    "GORDO":     ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    "EURO":      ["EUROMILLONES", "EURO MILLONES", "EURO"],
}

# Páginas "listado" para establecer contexto y cookies correctas antes de fetch()
LISTING_URL = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

# Endpoint JSON oficial (requiere cookie/sesión)
BASE_JSON = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# ------------------------------------------------

def _sleep(scale=1.0):
    time.sleep(BASE_SLEEP * scale + random.uniform(0, JITTER))

def _y_range():
    if INCR_DAYS > 0:
        # rango acotado por días
        today = date.today()
        start = today - timedelta(days=INCR_DAYS)
        return [(start.year, start, today)]
    else:
        return [(y, date(y,1,1), date(y,12,31)) for y in range(START_YEAR, END_YEAR+1)]

def normalize_draw(game_key, raw):
    # LAE devuelve estructuras variadas; intentamos cubrir los campos comunes
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or "").strip()
    if not fecha:
        return None

    numeros = []
    comb = raw.get("combinacion") or raw.get("combinacionNumeros") or raw.get("numeros") or raw.get("bolas") or ""
    if isinstance(comb, str):
        parts = [p for p in comb.replace(",", " ").replace("-", " ").split() if p.strip()]
        for p in parts:
            try: numeros.append(int(p))
            except: pass
    elif isinstance(comb, list):
        for p in comb:
            try: numeros.append(int(p))
            except: pass

    complementario = raw.get("complementario")
    reintegro = raw.get("reintegro")
    clave = raw.get("clave")
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }
    if complementario is not None: out["complementario"] = complementario
    if reintegro is not None:     out["reintegro"] = reintegro
    if clave is not None:         out["clave"] = clave
    stars = []
    if e1 is not None: stars.append(e1)
    if e2 is not None: stars.append(e2)
    if stars: out["estrellas"] = stars
    return out

def fetch_json_via_page(page, params):
    """
    Ejecuta fetch() desde el contexto de la página (misma-origin con cookie/sesión).
    Retorna JSON o levanta excepción con info de error.
    """
    js = """
    async (url, params) => {
      const q = new URLSearchParams(params);
      const res = await fetch(url + "?" + q.toString(), {
        method: "GET",
        headers: {
          "Accept": "application/json, text/plain, */*"
        },
        credentials: "include"
      });
      const text = await res.text();
      // La API devuelve JSON; si no, intentamos parsear, si falla, devolvemos error
      try {
        return { ok: res.ok, status: res.status, json: JSON.parse(text) };
      } catch (e) {
        return { ok: res.ok, status: res.status, error: "non-json", text };
      }
    }
    """
    return page.evaluate(js, BASE_JSON, params)

def fetch_year(page, game_key, variants, year, d1, d2):
    all_draws = []
    # Ir a la página de listados para ese juego: establece cookies y contexto
    page.goto(LISTING_URL[game_key], wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except PWTimeout:
        pass
    _sleep(1.2)

    for gid in variants:
        params = {
            "game_id": gid,
            "fechaInicioInclusiva": d1.isoformat(),
            "fechaFinInclusiva":   d2.isoformat()
        }
        res = fetch_json_via_page(page, params)
        if not res.get("ok"):
            # Rechazado por WAF o error; reportamos y probamos variante siguiente
            status = res.get("status")
            txt = (res.get("text") or "")[:120].replace("\n", "\\n")
            print(f"[warn] {game_key} {year} {gid} HTTP {status} head='{txt}'")
            _sleep(1.5)
            continue

        data = res.get("json") or {}
        items = data.get("busqueda") or data.get("sorteos") or data.get("resultados") or data.get("buscador") or []
        if isinstance(items, dict):
            for v in items.values():
                if isinstance(v, list):
                    items = v
                    break
        if not isinstance(items, list):
            items = []

        parsed = []
        for raw in items:
            d = normalize_draw(game_key, raw)
            if d: parsed.append(d)

        print(f"[sum] {game_key} {year} via '{gid}' => {len(parsed)} sorteos")
        all_draws.extend(parsed)
        if parsed:
            break  # con una variante válida nos vale
        _sleep(1.0)

    return all_draws

def latest_by_game(draws):
    def parse_d(d):
        s = d["date"]
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        return None
    res = {}
    for g, arr in draws.items():
        best, best_dt = None, None
        for d in arr:
            dt = parse_d(d)
            if dt and (best_dt is None or dt > best_dt):
                best_dt, best = dt, d
        if best:
            res[g] = best
    return res

def run():
    print("=== LAE · PRODUCCIÓN (runner propio) · start ===")
    print(f"[cfg] HEADLESS={HEADLESS}  RANGE={START_YEAR}..{END_YEAR}  INCR_DAYS={INCR_DAYS}")
    all_draws = {k: [] for k in GAMES}

    with sync_playwright() as pw:
        browser = pw.chromium.launch_persistent_context(
            USER_DATA_DIR.resolve().as_posix(),
            headless=HEADLESS,
            # viewport y locale razonables
            viewport={"width": 1280, "height": 900},
            locale="es-ES",
            user_agent=(
              "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
            ),
            # importante: no emular móvil / sin proxies raros
        )
        page = browser.new_page()

        # Ciclo por juego y rango temporal
        for game_key, variants in GAMES.items():
            print(f"[run] {game_key} :: {LISTING_URL[game_key]}")
            for (year, d1, d2) in _y_range():
                draws = fetch_year(page, game_key, variants, year, d1, d2)
                all_draws[game_key].extend(draws)
                _sleep(1.0)

        browser.close()

    # Persistir resultados
    generated_at = datetime.utcnow().isoformat() + "Z"
    flat = []
    for arr in all_draws.values():
        flat.extend(arr)

    payload = {
        "generated_at": generated_at,
        "results": flat,
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "lae_historico.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    for g, arr in all_draws.items():
        (OUT_DIR / f"{g}.json").write_text(json.dumps({"generated_at": generated_at, "results": arr}, ensure_ascii=False, indent=2), encoding="utf-8")

    latest = latest_by_game(all_draws)
    (OUT_DIR / "lae_latest.json").write_text(json.dumps({"generated_at": generated_at, "results": list(latest.values())}, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== LAE · PRODUCCIÓN · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    run()
