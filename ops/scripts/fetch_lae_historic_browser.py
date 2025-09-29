# ops/scripts/fetch_lae_historic_browser.py
# -*- coding: utf-8 -*-

import os, json, time, random
from datetime import datetime, date

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
OUT_DIR = os.path.join("docs", "api")

# Rango de años a capturar (rápido para producción reciente)
START_YEAR = 2020
END_YEAR   = date.today().year

BASE_API = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# Códigos *cortos* oficiales del parámetro game_id
# LP = La Primitiva, LN = Bonoloto, LE = El Gordo, EU = Euromillones
GAMES = {
    "PRIMITIVA": {"code": "LP", "referer": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos"},
    "BONOLOTO":  {"code": "LN", "referer": "https://www.loteriasyapuestas.es/es/bonoloto/sorteos"},
    "GORDO":     {"code": "LE", "referer": "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos"},
    "EURO":      {"code": "EU", "referer": "https://www.loteriasyapuestas.es/es/euromillones/sorteos"},
}

# Cabeceras razonables para el contexto. El Referer lo aporta la propia página.
CTX_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "es-ES,es;q=0.9",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "navigate",
    "sec-fetch-dest": "document",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    ),
}

# =========================
# UTILIDADES
# =========================
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def backoff_sleep(i, base=1.0, jitter=0.4):
    # 1.0, 2.0, 4.0, 8.0, ... con algo de "jitter"
    time.sleep(base * (2 ** (i - 1)) + random.uniform(0, jitter))

def normalize_draw(game_key, raw):
    """
    Convierte el objeto crudo del servicio LAE a un formato común.
    Campos que suelen aparecer: fechaSorteo / fecha_sorteo, combinacion,
    complementario, reintegro, clave, estrella1/estrella2, etc.
    """
    fecha = (
        raw.get("fecha_sorteo")
        or raw.get("fechaSorteo")
        or raw.get("fecha")
        or ""
    )
    if not fecha:
        return None

    # Números
    numeros = []
    comb = (
        raw.get("combinacion")
        or raw.get("combinacionNumeros")
        or raw.get("numeros")
        or raw.get("bolas")
        or ""
    )
    if isinstance(comb, str):
        parts = [p for p in comb.replace(",", " ").replace("-", " ").split() if p.strip()]
        for p in parts:
            try:
                numeros.append(int(p))
            except:
                pass
    elif isinstance(comb, list):
        for p in comb:
            try:
                numeros.append(int(p))
            except:
                pass

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }

    # Extras
    for k in ("complementario", "reintegro", "clave"):
        if raw.get(k) is not None:
            out[k] = raw.get(k)

    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    estrellas = []
    if e1 is not None: estrellas.append(e1)
    if e2 is not None: estrellas.append(e2)
    if estrellas:
        out["estrellas"] = estrellas

    return out

def latest_by_game(draws_by_game):
    """
    Devuelve el sorteo más reciente por juego.
    """
    def parse_date(s):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        return None

    latest = {}
    for g, arr in draws_by_game.items():
        best = None
        best_dt = None
        for d in arr:
            dt = parse_date(d["date"])
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt
                best = d
        if best:
            latest[g] = best
    return latest

# =========================
# FETCH VIA NAVEGADOR
# =========================
def fetch_year_via_browser(page, game_key, game_code, referer, year, tries=6):
    """
    Navega al referer del juego (para cookies/session) y ejecuta window.fetch
    al servicio JSON con cabeceras de XHR y credenciales incluidas.
    """
    start = f"{year}-01-01"
    end   = f"{year}-12-31"

    # Entramos al referer para establecer cookies y mismo origen
    try:
        page.goto(referer, wait_until="domcontentloaded", timeout=30000)
    except PWTimeout:
        pass  # la página de sorteos a veces tarda; seguimos igualmente

    url = f"{BASE_API}?game_id={game_code}&fechaInicioInclusiva={start}&fechaFinInclusiva={end}"

    for i in range(1, tries + 1):
        # Ejecutamos el fetch desde el contexto de la página (mismo origen)
        result = page.evaluate(
            """
            async (url) => {
              try {
                const resp = await fetch(url, {
                  method: 'GET',
                  headers: {
                    'accept': 'application/json, text/plain, */*',
                    'x-requested-with': 'XMLHttpRequest',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-dest': 'empty'
                  },
                  credentials: 'include',
                  cache: 'no-cache'
                });
                const text = await resp.text();
                return {status: resp.status, text};
              } catch (e) {
                return {status: 0, text: String(e)};
              }
            }
            """,
            url,
        )

        status = result.get("status", 0)
        text = (result.get("text") or "").strip()

        if status == 200 and text.startswith("{"):
            try:
                data = json.loads(text)
            except Exception as e:
                # JSON inválido: reintenta
                print(f"[retry] {game_key} {year} JSON parse error: {e}; sleeping… (attempt {i}/{tries})")
                backoff_sleep(i)
                continue

            # Encontrar la lista real de sorteos
            items = (
                data.get("busqueda")
                or data.get("sorteos")
                or data.get("resultados")
                or data.get("buscador")
                or []
            )
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
                if d and d["date"]:
                    parsed.append(d)

            print(f"[ok] {game_key} {year} -> {len(parsed)} sorteos")
            return parsed

        # 403 o error: backoff y reintento
        msg = f"HTTP {status}" if status else "network/error"
        print(f"[retry] {game_key} {year} --> {msg}; sleeping… (attempt {i}/{tries})")
        backoff_sleep(i)

    # Si no se pudo, devolvemos vacío
    print(f"[fail] {game_key} {year} -> sin datos tras reintentos")
    return []

def fetch_all_history_browser():
    """
    Recorre juegos y años vía navegador.
    """
    by_game = {k: [] for k in GAMES.keys()}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(locale="es-ES", extra_http_headers=CTX_HEADERS)
        page = context.new_page()

        print("=== LAE · HISTÓRICO (browser) · start ===")
        print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")

        for game_key, meta in GAMES.items():
            code = meta["code"]
            referer = meta["referer"]
            for year in range(START_YEAR, END_YEAR + 1):
                draws = fetch_year_via_browser(page, game_key, code, referer, year)
                by_game[game_key].extend(draws)

        context.close()
        browser.close()

    print("=== LAE · HISTÓRICO (browser) · done ===")
    return by_game

# =========================
# MAIN
# =========================
def main():
    ensure_dir(OUT_DIR)
    all_draws = fetch_all_history_browser()

    # payload maestro
    generated_at = datetime.utcnow().isoformat() + "Z"
    flat = []
    for arr in all_draws.values():
        flat.extend(arr)

    payload = {
        "generated_at": generated_at,
        "results": flat,
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    # maestro
    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # por juego
    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": generated_at, "results": arr}, f, ensure_ascii=False, indent=2)

    # latest
    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated_at, "results": list(latest.values())}, f, ensure_ascii=False, indent=2)

    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
