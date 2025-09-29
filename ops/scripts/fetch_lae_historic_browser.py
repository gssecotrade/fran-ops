# ops/scripts/fetch_lae_historic_browser.py
import os, json, re, time, random
from datetime import datetime, date
from typing import Any, Dict, List

# Config general
OUT_DIR = os.path.join("docs", "api")
START_YEAR = 2020                       # histórico desde 2020 (rápido para producción)
END_YEAR   = date.today().year

GAMES = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# ---------------- Playwright helpers ----------------
def _with_playwright():
    """Context manager perezoso para no importar playwright si no se usa desde CLI."""
    from playwright.sync_api import sync_playwright
    return sync_playwright()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def year_ok(iso_or_dmy: str) -> bool:
    """filtra por rango de años configurado."""
    y = None
    s = iso_or_dmy.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            y = datetime.strptime(s, fmt).year
            break
        except:
            pass
    if y is None:
        # intenta “YYYY” a pelo
        m = re.search(r"\b(19|20)\d{2}\b", s)
        if m:
            y = int(m.group(0))
    return (y is not None) and (START_YEAR <= y <= END_YEAR)

def normalize_draw(game_key: str, raw: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Dado un dict “raw” (procedente del estado pre-cargado o XHR),
    intenta normalizar a {game,date,numbers,complementario?,reintegro?,estrellas?}
    """
    # Posibles campos de fecha
    fecha = (
        raw.get("fecha_sorteo") or raw.get("fechaSorteo")
        or raw.get("fecha") or raw.get("date") or ""
    )
    if not isinstance(fecha, str) or not fecha:
        return None
    if not year_ok(fecha):
        return None

    # Combinación principal
    numeros: List[int] = []
    combo = (
        raw.get("combinacion") or raw.get("combinacionNumeros")
        or raw.get("numeros") or raw.get("bolas") or raw.get("numerosSorteo")
    )
    if isinstance(combo, str):
        parts = re.split(r"[^\d]+", combo)
        for p in parts:
            if p.isdigit():
                try: numeros.append(int(p))
                except: pass
    elif isinstance(combo, list):
        for v in combo:
            try: numeros.append(int(v))
            except: pass

    # Campos extra
    comp = raw.get("complementario")
    rein = raw.get("reintegro")
    clave = raw.get("clave")

    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    estrellas = []
    if e1 is not None: estrellas.append(e1)
    if e2 is not None: estrellas.append(e2)

    out = {"game": game_key, "date": fecha, "numbers": numeros[:6] if numeros else []}
    if comp is not None: out["complementario"] = comp
    if rein is not None: out["reintegro"] = rein
    if clave is not None: out["clave"] = clave
    if estrellas: out["estrellas"] = estrellas
    return out

def deep_find_drawish_dicts(obj: Any) -> List[Dict[str, Any]]:
    """
    Busca dentro de un JSON estructuras que “parezcan” sorteos:
    con fecha + combinación/bolas/… Devuelve lista de dicts candidatos.
    """
    found: List[Dict[str, Any]] = []

    def walk(node: Any):
        if isinstance(node, dict):
            keys = set(node.keys())
            has_fecha = any(k in keys for k in ["fecha", "fechaSorteo", "fecha_sorteo", "date"])
            has_combo = any(k in keys for k in ["combinacion", "combinacionNumeros", "numeros", "bolas", "numerosSorteo"])
            if has_fecha and has_combo:
                found.append(node)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)

    walk(obj)
    return found

# ----------------- Core scraping -----------------
def fetch_game_draws_from_page(game_key: str, url: str) -> List[Dict[str, Any]]:
    """
    Carga la página pública del juego, extrae window.__PRELOADED_STATE__ y/o XHRs JSON,
    y normaliza los sorteos al formato común.
    """
    with _with_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
        ])
        page = browser.new_page(
            user_agent=USER_AGENT,
            locale="es-ES",
            extra_http_headers={
                "Accept-Language": "es-ES,es;q=0.9",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

        # Captura XHR/JSON por si la página los usa en vez de estado pre-cargado
        xhr_json_blobs: List[Any] = []
        def on_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "application/json" in ctype and "/servicios/" in resp.url:
                    xhr_json_blobs.append(resp.json())
            except:
                pass
        page.on("response", on_response)

        # Navega y espera o bien “idle” o un selector típico del listado
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # margen para que dispare XHRs si los hubiera
        page.wait_for_timeout(1500)

        # 1) intenta leer el estado pre-cargado directamente desde JS (mejor que regex)
        state = None
        try:
            state = page.evaluate("() => window.__PRELOADED_STATE__ || null")
        except Exception:
            state = None

        # Fallback: si no hay estado, toma el HTML y busca con regex
        if not state:
            html = page.content()
            m = re.search(r"window\.__PRELOADED_STATE__\s*=\s*({.*?});", html, re.S)
            if m:
                try:
                    state = json.loads(m.group(1))
                except Exception:
                    state = None

        browser.close()

    candidates: List[Dict[str, Any]] = []
    if state:
        candidates.extend(deep_find_drawish_dicts(state))
    for blob in xhr_json_blobs:
        candidates.extend(deep_find_drawish_dicts(blob))

    # normaliza + filtra por año
    out: List[Dict[str, Any]] = []
    for raw in candidates:
        d = normalize_draw(game_key, raw)
        if d:
            out.append(d)

    # de-dup básico por (date, numbers)
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for d in out:
        key = (d["date"], tuple(d.get("numbers", [])))
        if key not in seen:
            seen.add(key)
            uniq.append(d)
    return uniq

def latest_by_game(draws: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Último sorteo por juego (para docs/api/lae_latest.json)."""
    res = []
    for g, arr in draws.items():
        def parse_dt(s: str):
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try: return datetime.strptime(s, fmt)
                except: pass
            return None
        best = None
        best_dt = None
        for d in arr:
            dt = parse_dt(d["date"])
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt; best = d
        if best: res.append(best)
    return res

def main():
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")
    ensure_dir(OUT_DIR)

    all_by_game: Dict[str, List[Dict[str, Any]]] = {k: [] for k in GAMES.keys()}

    for game, url in GAMES.items():
        print(f"[run] {game} :: {url}")
        try:
            draws = fetch_game_draws_from_page(game, url)
            print(f"[sum] {game} -> {len(draws)} sorteos")
            all_by_game[game] = draws
        except Exception as e:
            print(f"[warn] {game} -> error {e}")
        # pequeñísima pausa (humano)
        time.sleep(0.5 + random.uniform(0, 0.4))

    # payload maestro
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_by_game.values(), []),  # lista plana
        "by_game_counts": {k: len(v) for k, v in all_by_game.items()},
        "meta": {"from_year": START_YEAR, "to_year": END_YEAR}
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # particionado por juego (útil para Apps Script)
    for g, arr in all_by_game.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": payload["generated_at"], "results": arr}, f, ensure_ascii=False, indent=2)

    # latest
    latest = latest_by_game(all_by_game)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": payload["generated_at"], "results": latest}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
