# ops/scripts/fetch_lae_historic_browser.py
# Scrape "oficial" LAE usando Playwright:
# - Abre la web de cada juego para obtener cookies/sesión.
# - Llama al endpoint XHR de LAE DESDE el contexto de la página (sin 403).
# - Normaliza y guarda: docs/api/lae_historico.json, lae_latest.json y particionados.

import os, json, time, math
from datetime import date, datetime
from typing import Dict, List, Any

# Playwright (API síncrona)
from playwright.sync_api import sync_playwright

# ================== CONFIG ==================
OUT_DIR = os.path.join("docs", "api")

# Años que quieres cubrir (ajusta según tu pipeline)
START_YEAR = 2020       # <<< pide últimos 5 años
END_YEAR   = date.today().year

# Mapa de juegos -> variantes admitidas por LAE en "game_id"
# (probamos en este orden hasta que devuelva datos)
GAMES: Dict[str, List[str]] = {
    "PRIMITIVA": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    "BONOLOTO" : ["BONOLOTO"],
    "GORDO"    : ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    "EURO"     : ["EUROMILLONES", "EURO MILLONES", "EURO"],
}

# URLs públicas del portal (para levantar sesión/cookies)
PORTAL_PAGES = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO" : "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO"    : "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO"     : "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

# Endpoint real que usa la web
XHR_BASE = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# Timeout de navegación/llamadas
NAV_TIMEOUT_MS = 40_000


# ============== UTILIDADES =================
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_draw(game_key: str, raw: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Convierte la respuesta cruda de LAE a un formato estable:
    {
      game, date, numbers[...], complementario?, reintegro?, claves?, estrellas?
    }
    """
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or "").strip()
    if not fecha:
        return None

    # números (pueden venir como lista o string)
    numeros: List[int] = []
    comb = raw.get("combinacion") or raw.get("combinacionNumeros") or raw.get("numeros") or raw.get("bolas") or ""
    if isinstance(comb, list):
        for x in comb:
            try:
                numeros.append(int(x))
            except Exception:
                pass
    elif isinstance(comb, str):
        parts = [p for p in comb.replace(",", " ").replace("-", " ").split() if p.strip()]
        for p in parts:
            try:
                numeros.append(int(p))
            except Exception:
                pass

    out: Dict[str, Any] = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }

    # campos opcionales (según juego)
    if raw.get("complementario") is not None:
        out["complementario"] = raw.get("complementario")

    if raw.get("reintegro") is not None:
        out["reintegro"] = raw.get("reintegro")

    # "clave" (El Gordo)
    if raw.get("clave") is not None:
        out["clave"] = raw.get("clave")

    # Estrellas (Euromillones)
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    est = [e for e in (e1, e2) if e is not None]
    if est:
        out["estrellas"] = est

    return out


def flatten(list_of_lists: List[List[Any]]) -> List[Any]:
    res: List[Any] = []
    for sub in list_of_lists:
        res.extend(sub)
    return res


def latest_by_game(all_draws: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    Devuelve el sorteo más reciente por juego (según fecha).
    """
    def parse_dt(s: str) -> datetime | None:
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        return None

    latest: Dict[str, Dict[str, Any]] = {}
    for g, arr in all_draws.items():
        best = None
        best_dt = None
        for d in arr:
            dt = parse_dt(d.get("date", "")) if d else None
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt
                best = d
        if best:
            latest[g] = best
    return latest


# ============ CORE: scraping con Playwright ============
def fetch_year_with_browser(page, game_key: str, game_id: str, year: int) -> List[Dict[str, Any]]:
    """
    Ejecuta DESDE el contexto de la página (con cookies/origin de LAE) una llamada fetch()
    al endpoint XHR oficial para el año indicado. Devuelve lista normalizada de sorteos.
    """
    start = f"{year}-01-01"
    end   = f"{year}-12-31"

    url = (
        f"{XHR_BASE}"
        f"?game_id={game_id.replace(' ', '%20')}"
        f"&fechaInicioInclusiva={start}"
        f"&fechaFinInclusiva={end}"
    )

    # Hacemos fetch dentro de la página para heredar cookies/origin y evitar 403
    js = """
        async (url) => {
          const res = await fetch(url, {
            method: 'GET',
            credentials: 'include',
            headers: {
              'Accept': 'application/json, text/plain, */*'
            }
          });
          const txt = await res.text();
          // El servicio devuelve JSON; si por lo que sea llega HTML, devolvemos objeto vacío
          try { return JSON.parse(txt); } catch (e) { return {error: 'non-json', text: txt}; }
        }
    """

    # Intentos con backoff suave (anti intermitencias)
    tries = 4
    last_err = None
    for i in range(1, tries + 1):
        try:
            data = page.evaluate(js, url)
            if isinstance(data, dict) and ("error" not in data):
                items = (
                    data.get("busqueda")
                    or data.get("sorteos")
                    or data.get("resultados")
                    or data.get("buscador")
                    or []
                )
                if isinstance(items, dict):
                    # buscar la primera lista dentro
                    found = None
                    for v in items.values():
                        if isinstance(v, list):
                            found = v
                            break
                    items = found or []
                if not isinstance(items, list):
                    items = []

                out: List[Dict[str, Any]] = []
                for raw in items:
                    d = normalize_draw(game_key, raw)
                    if d:
                        out.append(d)
                return out
            else:
                last_err = f"Respuesta no JSON o con error: {str(data)[:160]}"
        except Exception as e:
            last_err = str(e)

        # backoff modesto
        time.sleep(0.6 * i)

    print(f"[warn] {game_key} {year} '{game_id}' -> no datos ({last_err})")
    return []


def fetch_game_history(page, game_key: str) -> List[Dict[str, Any]]:
    """
    Abre la página pública del juego (cookies/sesión) y consulta año a año con
    las variantes de game_id hasta conseguir resultados o agotar variantes.
    """
    url = PORTAL_PAGES[game_key]
    print(f"[run] {game_key} :: {url}")
    page.goto(url, timeout=NAV_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded")

    all_years: List[Dict[str, Any]] = []
    variants = GAMES[game_key]

    for year in range(START_YEAR, END_YEAR + 1):
        got: List[Dict[str, Any]] = []
        for gid in variants:
            got = fetch_year_with_browser(page, game_key, gid, year)
            if got:  # si hay resultados, no probamos más variantes ese año
                break
        print(f"[sum] {game_key} {year} -> {len(got)} sorteos")
        all_years.extend(got)

    return all_years


def main() -> None:
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")
    ensure_dir(OUT_DIR)

    all_draws: Dict[str, List[Dict[str, Any]]] = {k: [] for k in GAMES.keys()}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(locale="es-ES")
        page = context.new_page()

        # PRIMITIVA, BONOLOTO, GORDO, EURO
        for game_key in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
            try:
                draws = fetch_game_history(page, game_key)
                all_draws[game_key] = draws
            except Exception as e:
                print(f"[err] {game_key}: {e}")
                all_draws[game_key] = []
            # respirito entre juegos (evita rate-limit)
            time.sleep(0.8)

        browser.close()

    # ---------- Persistencia ----------
    generated = datetime.utcnow().isoformat() + "Z"

    # Maestro conjunto
    payload = {
        "generated_at": generated,
        "results": flatten(list(all_draws.values())),
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }
    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Particiones por juego
    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": generated, "results": arr}, f, ensure_ascii=False, indent=2)

    # Latest por juego
    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated, "results": list(latest.values())}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])


if __name__ == "__main__":
    main()
