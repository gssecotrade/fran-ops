# ops/scripts/fetch_lae_spider.py
import os, json, asyncio, math, random
from datetime import date, datetime
from typing import Dict, Any, List

from playwright.sync_api import sync_playwright

OUT_DIR = os.path.join("docs", "api")
os.makedirs(OUT_DIR, exist_ok=True)

# Rango a cubrir (ajústalo si quieres)
START_YEAR = 2020
END_YEAR   = date.today().year

# Mapa de juegos → (url pública para referer/same-origin, game_id para API)
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
    # fecha*
    fecha = (
        raw.get("fecha_sorteo")
        or raw.get("fechaSorteo")
        or raw.get("fecha")
        or ""
    )
    if not fecha:
        return None

    # Números: muchos formatos posibles
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
            try:
                nums.append(int(p))
            except:
                pass
    elif isinstance(comb, list):
        for p in comb:
            try:
                nums.append(int(p))
            except:
                pass

    out: Dict[str, Any] = {
        "game": game_key,
        "date": fecha,
        "numbers": nums[:6] if nums else [],
    }
    # Campos adicionales si existen
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
    def parse_dt(s: str):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        return None

    latest = []
    for g, arr in draws_by_game.items():
        best = None
        best_dt = None
        for d in arr:
            dt = parse_dt(d.get("date", "")) or best_dt
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt
                best = d
        if best:
            latest.append(best)
    return latest

def fetch_json_same_origin(page, base_url: str, params: Dict[str, str]) -> Dict[str, Any] | None:
    """
    Llama a /servicios/buscadorSorteos desde el propio contexto de la página.
    Evita 403 porque es same-origin con cookies/headers correctos.
    """
    # Monta query string
    from urllib.parse import urlencode
    qs = urlencode(params, safe="% ")
    url = SERVICE_PATH + "?" + qs

    script = f"""
        const url = {json.dumps(url)};
        try {{
          const r = await fetch(url, {{
            method: 'GET',
            credentials: 'include',
            headers: {{
              'Accept': 'application/json, text/plain, */*'
            }}
          }});
          const txt = await r.text();
          if (!txt || txt.trim().length === 0) return null;
          try {{
            return {{ ok: r.ok, status: r.status, body: JSON.parse(txt) }};
          }} catch(e) {{
            return {{ ok: r.ok, status: r.status, body: null, raw: txt }};
          }}
        }} catch(e) {{
          return {{ ok: false, status: 0, err: String(e) }};
        }}
    """
    res = page.evaluate(script)
    if not res or not res.get("ok"):
        return None
    body = res.get("body")
    return body

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

            # Abre la página pública del juego (establece same-origin cookies/headers)
            page.goto(list_url, wait_until="domcontentloaded", timeout=60000)

            total_game = 0
            for year in range(START_YEAR, END_YEAR + 1):
                start = f"{year}-01-01"
                end   = f"{year}-12-31"

                got_year = False
                for gid in variants:
                    params = {
                        "game_id": gid,
                        "fechaInicioInclusiva": start,
                        "fechaFinInclusiva": end,
                    }
                    data = fetch_json_same_origin(page, list_url, params)
                    if not data:
                        # pequeño backoff y prueba siguiente variante
                        page.wait_for_timeout(300 + int(200*random.random()))
                        continue

                    # La estructura exacta puede variar. Buscamos la lista.
                    items = (
                        data.get("busqueda")
                        or data.get("sorteos")
                        or data.get("resultados")
                        or data.get("buscador")
                        or []
                    )
                    if isinstance(items, dict):
                        # si es dict, toma la primera lista que encuentres
                        lst = None
                        for v in items.values():
                            if isinstance(v, list):
                                lst = v; break
                        items = lst or []

                    parsed = []
                    for raw in items if isinstance(items, list) else []:
                        d = _normalize_draw(game_key, raw)
                        if d: parsed.append(d)

                    if parsed:
                        all_draws[game_key].extend(parsed)
                        total_game += len(parsed)
                        got_year = True
                        break  # esa variante funcionó para ese año

                    # si no hubo datos, prueba la siguiente variante tras un pequeño delay
                    page.wait_for_timeout(200 + int(200*random.random()))

                # pausa corta entre años para no ser agresivos
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

    # Guarda maestro
    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Particiones por juego
    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": generated_at, "results": arr}, f, ensure_ascii=False, indent=2)

    # Latest
    latest = _latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated_at, "results": latest}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (spider via same-origin JSON) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    run_spider()
