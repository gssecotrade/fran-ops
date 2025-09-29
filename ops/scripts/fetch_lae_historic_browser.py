# ops/scripts/fetch_lae_historic_browser.py
import os, json, time, random
from datetime import datetime, date
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

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
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or "").strip()
    if not fecha: return None
    nums = []
    comb = (raw.get("combinacion") or raw.get("combinacionNumeros")
            or raw.get("numeros") or raw.get("bolas") or "")
    if isinstance(comb, str):
        for p in comb.replace(",", " ").replace("-", " ").split():
            try: nums.append(int(p))
            except: pass
    elif isinstance(comb, list):
        for p in comb:
            try: nums.append(int(p))
            except: pass
    out = {"game": game_key, "date": fecha, "numbers": nums[:6] if nums else []}
    for k in ("complementario", "reintegro", "clave"):
        if raw.get(k) is not None: out[k] = raw.get(k)
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

JS_FETCH = """
async ({base, params}) => {
  const url = new URL(base);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const res = await fetch(url.toString(), {
    method: 'GET',
    credentials: 'include',
    headers: {
      'accept': 'application/json, text/plain, */*'
    }
  });
  if (!res.ok) {
    return { ok:false, status: res.status, text: await res.text() };
  }
  const text = await res.text();
  return { ok:true, text };
}
"""

def prime_consent(page):
    # intenta aceptar consent en distintas variantes
    for sel in (
        'button:has-text("Aceptar todo")',
        'button:has-text("Aceptar")',
        'button[aria-label="Aceptar"]',
    ):
        try:
            page.locator(sel).first.click(timeout=1500)
            break
        except PwTimeout:
            pass
        except:
            pass

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

        for game_key, variants in GAMES.items():
            referer = REFERERS[game_key]
            page = context.new_page()
            page.goto(referer, wait_until="domcontentloaded", timeout=45000)
            prime_consent(page)
            time.sleep(0.8)  # asegura persistencia de cookies

            for year in range(START_YEAR, END_YEAR+1):
                start = f"{year}-01-01"; end = f"{year}-12-31"
                got = False
                for gid in variants:
                    params = {"game_id": gid, "fechaInicioInclusiva": start, "fechaFinInclusiva": end}

                    # retry en la propia página (CREDENCIALES INCLUIDAS)
                    last = "unknown"
                    for attempt in range(1, 7):
                        try:
                            res = page.evaluate(JS_FETCH, {"base": BASE, "params": params})
                            if res and res.get("ok"):
                                text = res.get("text", "").strip()
                                if text.startswith("{"):
                                    data = json.loads(text)
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
                                        last = "empty-list"
                                else:
                                    last = "bad-body"
                            else:
                                last = f"HTTP {res.get('status') if res else 'err'}"
                        except Exception as e:
                            last = str(e)

                        # si no ok, pequeño backoff y refresco ligero de la página (mantiene cookies)
                        sleep = (2 ** (attempt-1)) + random.uniform(0, 0.6)
                        print(f"[retry] {game_key} {year} ({gid}) -> {last}; sleeping {sleep:.1f}s (attempt {attempt}/6)")
                        time.sleep(sleep)
                        try:
                            page.reload(wait_until="domcontentloaded", timeout=30000)
                            prime_consent(page)
                        except:
                            pass

                    if got: break
                    time.sleep(0.4 + random.uniform(0, 0.5))

                if not got:
                    print(f"[info] {game_key} {year} → 0 sorteos (todas variantes fallaron)")
                time.sleep(0.2 + random.uniform(0, 0.3))

            page.close()

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
        json.dump({"generated_at": payload["generated_at"], "results": list(latest.values())},
                  f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
