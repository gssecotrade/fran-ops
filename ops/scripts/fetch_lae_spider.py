# ops/scripts/fetch_lae_spider.py
import os, json, random
from datetime import date, datetime
from typing import Dict, Any, List, Tuple
from playwright.sync_api import sync_playwright

OUT_DIR = os.path.join("docs", "api")
os.makedirs(OUT_DIR, exist_ok=True)

START_YEAR = 2020
END_YEAR   = date.today().year

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

# ---------- util ----------

def _normalize_draw(game_key: str, raw: Dict[str, Any]) -> Dict[str, Any] | None:
    fecha = raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or ""
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
    def parse_dt(s: str):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try: return datetime.strptime(s, fmt)
            except: pass
        return None
    latest = []
    for _, arr in draws_by_game.items():
        best, best_dt = None, None
        for d in arr:
            dt = parse_dt(d.get("date", "")) or best_dt
            if dt and (best_dt is None or dt > best_dt):
                best_dt, best = dt, d
        if best: latest.append(best)
    return latest

def fetch_json_same_origin(page, rel_url: str) -> Dict[str, Any] | None:
    """Hace fetch desde el contexto del site (resuelve CORS y cookies)."""
    return page.evaluate(
        """
        async (url) => {
          try {
            const r = await fetch(url, {
              method: 'GET',
              credentials: 'include',
              headers: {
                'Accept': 'application/json, text/plain, */*',
                'X-Requested-With': 'XMLHttpRequest'
              }
            });
            const txt = await r.text();
            const head = txt ? txt.slice(0, 160) : '';
            try {
              return { ok: r.ok, status: r.status, body: JSON.parse(txt), head };
            } catch {
              return { ok: r.ok, status: r.status, body: null, head };
            }
          } catch (e) {
            return { ok: false, status: 0, err: String(e) };
          }
        }
        """,
        rel_url,
    )

def _param_variants(start: str, end: str) -> List[Tuple[str, str]]:
    """
    Genera combinaciones plausibles de nombres/formatos de fechas
    que LAE ha usado en distintas versiones del buscador.
    """
    yyyy_mm = (start, end)                             # '2020-01-01'
    dd_mm   = tuple("-".join(reversed(d.split("-")))   # '01-01-2020'
                    for d in (start, end))
    dd_mm_slash = tuple(s.replace("-", "/") for s in dd_mm)  # '01/01/2020'

    combos = []
    for k1, k2 in [
        ("fechaInicioInclusiva", "fechaFinInclusiva"),
        ("fechaInicio",          "fechaFin"),
        ("desde",                "hasta"),
    ]:
        for a, b in (yyyy_mm, dd_mm, dd_mm_slash):
            combos.append((f"{k1}={a}&{k2}={b}", f"{k1}", f"{k2}"))
    return [(q, "", "") for (q, _, __) in combos]  # la cadena query ya está formada

def _build_queries(game_id: str, start: str, end: str) -> List[str]:
    qs = [f"game_id={game_id}&{tail}" for (tail, _, __) in _param_variants(start, end)]
    # dedup conservando orden
    seen, out = set(), []
    for q in qs:
        if q not in seen:
            out.append(q); seen.add(q)
    return out

# ---------- main ----------

def run_spider():
    print("=== LAE · HISTÓRICO (spider via same-origin JSON) · start ===")
    all_draws: Dict[str, List[Dict[str, Any]]] = {k: [] for k in GAMES.keys()}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-gpu", "--no-sandbox"])
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        )
        page = ctx.new_page()

        for game_key, meta in GAMES.items():
            page.goto(meta["list_url"], wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(600)

            total_game = 0
            for year in range(START_YEAR, END_YEAR + 1):
                start = f"{year}-01-01"
                end   = f"{year}-12-31"

                got_year = False
                for gid in meta["ids"]:
                    # probamos múltiples combinaciones de parámetros/formatos
                    for q in _build_queries(gid, start, end):
                        rel = f"{SERVICE_PATH}?{q}"
                        res = fetch_json_same_origin(page, rel)

                        if not res:
                            print(f"[warn] {game_key} {year} ({gid}) -> sin respuesta")
                            continue

                        if not res.get("ok"):
                            # log conciso para depurar sin romper
                            print(f"[warn] {game_key} {year} ({gid}) "
                                  f"HTTP {res.get('status')} head='{(res.get('head') or '')[:80]}'")
                            page.wait_for_timeout(180 + int(220*random.random()))
                            continue

                        data = res.get("body")
                        items = None
                        if isinstance(data, dict):
                            # buscamos la primera lista que parezca los sorteos
                            for k, v in data.items():
                                if isinstance(v, list): items = v; break
                            if items is None:
                                for v in data.values():
                                    if isinstance(v, dict):
                                        for k2, v2 in v.items():
                                            if isinstance(v2, list): items = v2; break
                                    if items is not None: break
                        if not isinstance(items, list):
                            items = []

                        parsed = []
                        for raw in items:
                            d = _normalize_draw(game_key, raw)
                            if d: parsed.append(d)

                        if parsed:
                            all_draws[game_key].extend(parsed)
                            total_game += len(parsed)
                            got_year = True
                            break  # no probar más combinaciones para este gid/año

                        page.wait_for_timeout(120 + int(160*random.random()))
                    if got_year:
                        break  # pasamos al año siguiente

                page.wait_for_timeout(150 + int(200*random.random()))

            print(f"[sum] {game_key} => {total_game} sorteos")
        browser.close()

    generated_at = datetime.utcnow().isoformat() + "Z"
    flat = [d for arr in all_draws.values() for d in arr]

    payload = {"generated_at": generated_at,
               "results": flat,
               "by_game_counts": {k: len(v) for k, v in all_draws.items()}}

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
