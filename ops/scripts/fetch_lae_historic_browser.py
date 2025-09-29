# ops/scripts/fetch_lae_historic_browser.py
import os, json, time, random
from datetime import datetime, date
from playwright.sync_api import sync_playwright

OUT_DIR = os.path.join("docs", "api")

GAMES = {
    "PRIMITIVA": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    "BONOLOTO":  ["BONOLOTO"],
    "GORDO":     ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    "EURO":      ["EUROMILLONES", "EURO MILLONES", "EURO"]
}

REFERERS = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

START_YEAR = int(os.getenv("LAE_START_YEAR", "2020"))
END_YEAR   = date.today().year

BASE = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def normalize_draw(game_key, raw):
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo")
             or raw.get("fecha") or "").strip()
    if not fecha:
        return None
    numeros = []
    comb = (raw.get("combinacion") or raw.get("combinacionNumeros")
            or raw.get("numeros") or raw.get("bolas") or "")
    if isinstance(comb, str):
        parts = [p for p in comb.replace(",", " ").replace("-", " ").split() if p.strip()]
        for p in parts:
            try: numeros.append(int(p))
            except: pass
    elif isinstance(comb, list):
        for p in comb:
            try: numeros.append(int(p))
            except: pass

    out = {"game": game_key, "date": fecha, "numbers": numeros[:6] if numeros else []}
    for k in ("complementario", "reintegro", "clave"):
        if raw.get(k) is not None:
            out[k] = raw.get(k)
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    estrellas = []
    if e1 is not None: estrellas.append(e1)
    if e2 is not None: estrellas.append(e2)
    if estrellas: out["estrellas"] = estrellas
    return out

def latest_by_game(draws):
    def parse_dt(s):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try: return datetime.strptime(s, fmt)
            except: pass
        return None
    res = {}
    for g, arr in draws.items():
        best, best_dt = None, None
        for d in arr:
            dt = parse_dt(d["date"])
            if dt and (best_dt is None or dt > best_dt):
                best_dt, best = dt, d
        if best: res[g] = best
    return res

def main():
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")
    ensure_dir(OUT_DIR)
    all_draws = {k: [] for k in GAMES.keys()}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=UA,
            locale="es-ES",
            viewport={"width": 1366, "height": 900}
        )
        # Cabeceras globales del contexto para todas las peticiones
        context.set_extra_http_headers({
            "accept": "application/json, text/plain, */*",
            "accept-language": "es-ES,es;q=0.9,en;q=0.8",
            "origin": "https://www.loteriasyapuestas.es",
        })
        client = context.request  # APIRequestContext ligado al contexto (hereda UA/cookies)

        def prime_referer(ref_url: str):
            """Carga la página del juego y acepta el banner si aparece; así fijamos cookies."""
            page = context.new_page()
            page.goto(ref_url, wait_until="domcontentloaded", timeout=45000)
            # intentar aceptar consentimiento si existe
            for sel in ('button:has-text("Aceptar")',
                        'button:has-text("Aceptar todo")',
                        'button[aria-label="Aceptar"]'):
                try:
                    page.locator(sel).first.click(timeout=2000)
                    break
                except:
                    pass
            # pequeña espera para que se persistan cookies
            time.sleep(0.8)
            page.close()

        def retry_get(params, referer, tries=6):
            last = None
            for i in range(1, tries+1):
                try:
                    r = client.get(
                        BASE,
                        params=params,
                        headers={"referer": referer},
                        timeout=30000
                    )
                    if r.ok:
                        txt = r.text()
                        if txt.strip().startswith("{"):
                            return r.json()
                        last = "bad-body"
                    else:
                        last = f"HTTP {r.status}"
                except Exception as e:
                    last = str(e)
                # antes de reintentar, volvemos a primar referer (refresca cookies/consent)
                prime_referer(referer)
                sleep = (2 ** (i-1)) + random.uniform(0, 0.7)
                print(f"[retry] 403/err detected, sleeping {sleep:.1f}s (attempt {i}/{tries})")
                time.sleep(sleep)
            raise RuntimeError(f"Fallo GET JSON: {BASE}?{params} ({last})")

        # Recorremos juegos y años
        for game_key, variants in GAMES.items():
            referer = REFERERS[game_key]
            prime_referer(referer)
            for year in range(START_YEAR, END_YEAR+1):
                start = f"{year}-01-01"; end = f"{year}-12-31"
                got = False
                for gid in variants:
                    params = {"game_id": gid,
                              "fechaInicioInclusiva": start,
                              "fechaFinInclusiva": end}
                    try:
                        data = retry_get(params, referer)
                        items = (data.get("busqueda") or data.get("sorteos")
                                 or data.get("resultados") or data.get("buscador") or [])
                        if isinstance(items, dict):
                            for v in items.values():
                                if isinstance(v, list):
                                    items = v; break
                        if not isinstance(items, list): items = []

                        parsed = []
                        for raw in items:
                            d = normalize_draw(game_key, raw)
                            if d: parsed.append(d)

                        if parsed:
                            print(f"[ok] {game_key} {year} via '{gid}' -> {len(parsed)} sorteos")
                            all_draws[game_key].extend(parsed)
                            got = True
                            break
                        else:
                            print(f"[warn] {game_key} {year} '{gid}' sin sorteos (lista vacía)")
                    except Exception as e:
                        print(f"[fail] {game_key} {year} ({gid}): {e}")
                    time.sleep(0.5 + random.uniform(0, 0.5))
                if not got:
                    print(f"[info] {game_key} {year} → 0 sorteos (todas variantes fallaron)")
                time.sleep(0.2 + random.uniform(0, 0.3))
        browser.close()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_draws.values(), []),
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }
    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": payload["generated_at"],
                   "results": list(latest.values())}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
