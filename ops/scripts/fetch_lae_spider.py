# ops/scripts/fetch_lae_spider.py
import os, json, random
from datetime import date, datetime
from typing import Dict, Any, List
from playwright.sync_api import sync_playwright

OUT_DIR = os.path.join("docs", "api")
os.makedirs(OUT_DIR, exist_ok=True)

# RANGO (ajústalo si quieres)
START_YEAR = 2020
END_YEAR   = date.today().year

# Mapas de juegos: URL pública (para same-origin) + variantes de game_id del servicio
GAMES = {
    "PRIMITIVA": {
        "list_url": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
        "ids": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    },
    "BONOLOTO": {
        "list_url": "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
        "ids": ["BONOLOTO"],
    },
    "GORDO": {
        "list_url": "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
        "ids": ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    },
    "EURO": {
        "list_url": "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
        "ids": ["EUROMILLONES", "EURO MILLONES", "EURO"],
    },
}

SERVICE_PATH = "/servicios/buscadorSorteos"

def _normalize_draw(game_key: str, raw: Dict[str, Any]) -> Dict[str, Any] | None:
    fecha = (
        raw.get("fecha_sorteo")
        or raw.get("fechaSorteo")
        or raw.get("fecha")
        or ""
    )
    if not fecha:
        return None

    nums: List[int] = []
    comb = (
        raw.get("combinacion")
        or raw.get("combinacionNumeros")
        or raw.get("numeros")
        or raw.get("bolas")
        or ""
    )
    if isinstance(comb, str):
        for p in comb.replace(",", " ").replace("-", " ").split():
            try: nums.append(int(p))
            except: pass
    elif isinstance(comb, list):
        for p in comb:
            try: nums.append(int(p))
            except: pass

    out: Dict[str, Any] = {"game": game_key, "date": fecha, "numbers": nums[:6] if nums else []}
    for k in ("complementario", "reintegro", "clave"):
        if k in raw and raw[k] is not None:
            out[k] = raw[k]
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    stars = []
    if e1 is not None: stars.append(e1)
    if e2 is not None: stars.append(e2)
    if stars: out["estrellas"] = stars
    return out

def _latest_by_game(draws_by_game: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    from datetime import datetime
    def parse_dt(s: str):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try: return datetime.strptime(s, fmt)
            except: pass
        return None

    latest = []
    for g, arr in draws_by_game.items():
        best = None; best_dt = None
        for d in arr:
            dt = parse_dt(d.get("date", "")) or best_dt
            if dt and (best_dt is None or dt > best_dt):
                best_dt, best = dt, d
        if best: latest.append(best)
    return latest

def fetch_json_same_origin(page, rel_url: str) -> Dict[str, Any] | None:
    """
    Llama al JSON desde el contexto de la página (same-origin).
    ***CORREGIDO***: usamos evaluate con función async.
    """
    return page.evaluate(
        """
        async (url) => {
          try {
            const r = await fetch(url, {
              method: 'GET',
              credentials: 'include',
              headers: { 'Accept': 'application/json, text/plain, */*' }
            });
            const txt = await r.text();
            if (!txt || txt.trim().length === 0) return null;
            try {
              return { ok: r.ok, status: r.status, body: JSON.parse(txt) };
            } catch (e) {
              return { ok: r.ok, status: r.status, body: null, raw: txt };
            }
          } catch (e) {
            return { ok: false, status: 0, err: String(e) };
          }
        }
        """,
        rel_url,
    )

def run_spider():
    print("=== LAE · HISTÓRICO (spider via same-origin JSON) · start ===")
    all_draws: Dict[str, List[Dict[str, Any]]] = {k: [] for k in GAMES.keys()}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-gpu", "--no-sandbox"])
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()

        for game_key, meta in GAMES.items():
            list_url = meta["list_url"]
            variants = meta["ids"]

            # Abre la página pública para asentar cookies y contexto
            page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
            # pequeña espera adicional para scripts internos
            page.wait_for_timeout(500)

            total_game = 0
            for year in range(START_YEAR, END_YEAR + 1):
                start = f"{year}-01-01"
                end   = f"{year}-12-31"

                got_year = False
                for gid in variants:
                    from urllib.parse import urlencode
                    rel = SERVICE_PATH + "?" + urlencode(
                        {"game_id": gid, "fechaInicioInclusiva": start, "fechaFinInclusiva": end},
                        safe="% "
                    )

                    res = fetch_json_same_origin(page, rel)
                    if not res or not res.get("ok"):
                        page.wait_for_timeout(200 + int(200*random.random()))
                        continue

                    data = res.get("body")
                    items = (
                        (data or {}).get("busqueda")
                        or (data or {}).get("sorteos")
                        or (data or {}).get("resultados")
                        or (data or {}).get("buscador")
                        or []
                    )
                    if isinstance(items, dict):
                        lst = None
                        for v in items.values():
                            if isinstance(v, list): lst = v; break
                        items = lst or []

                    parsed = []
                    for raw in items if isinstance(items, list) else []:
                        d = _normalize_draw(game_key, raw)
                        if d: parsed.append(d)

                    if parsed:
                        all_draws[game_key].extend(parsed)
                        total_game += len(parsed)
                        got_year = True
                        break

                    page.wait_for_timeout(200 + int(200*random.random()))

                # pausa entre años
                page.wait_for_timeout(200 + int(200*random.random()))

            print(f"[sum] {game_key} => {total_game} sorteos")
        browser.close()

    generated_at = datetime.utcnow().isoformat() + "Z"
    flat = []
    for arr in all_draws.values():
        flat.extend(arr)

    payload = {
        "generated_at": generated_at,
        "results": flat,
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": generated_at, "results": arr}, f, ensure_ascii=False, indent=2)

    latest = _latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated_at, "results": latest}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (spider via same-origin JSON) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    run_spider()
