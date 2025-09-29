# ops/scripts/fetch_lae_historic_browser.py
import os, json, time, random
from datetime import datetime, date
from pathlib import Path

from playwright.sync_api import sync_playwright

# ========= Config =========
OUT_DIR = Path("docs/api")
OUT_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2020                      # <-- histórico desde 2020
END_YEAR   = date.today().year

# Página “host” (para que el fetch sea same-origin y no nos bloquee el WAF)
HOST_PAGE = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

# Variantes de game_id que acepta el backend (probamos de mayor a menor probabilidad)
GAME_IDS = {
    "PRIMITIVA": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    "BONOLOTO":  ["BONOLOTO"],
    "GORDO":     ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    "EURO":      ["EUROMILLONES", "EURO MILLONES", "EURO"],
}

BASE_JSON = "/servicios/buscadorSorteos"  # mismo host que la página

# ========= Utils =========
def jitter(a=0.25, b=0.6):
    time.sleep(random.uniform(a, b))

def normalize_draw(game_key: str, raw: dict):
    # fechas: fecha_sorteo / fechaSorteo / fecha
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

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }
    for k in ("complementario", "reintegro", "clave"):
        if raw.get(k) is not None:
            out[k] = raw.get(k)

    # Euromillones estrellas
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")
    est = []
    if e1 is not None: est.append(e1)
    if e2 is not None: est.append(e2)
    if est: out["estrellas"] = est
    return out


def latest_by_game(draws_by_game: dict):
    def parse(s):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try: return datetime.strptime(s, fmt)
            except: pass
        return None

    latest = {}
    for g, arr in draws_by_game.items():
        best, best_dt = None, None
        for d in arr:
            dt = parse(d["date"])
            if dt and (best_dt is None or dt > best_dt):
                best_dt, best = dt, d
        if best: latest[g] = best
    return latest


# ========= Core (Playwright) =========
def fetch_year_from_page(page, game_key: str, year: int) -> list:
    """Lanza el fetch DESDE EL NAVEGADOR (same-origin), probando variantes de game_id."""
    start = f"{year}-01-01"
    end   = f"{year}-12-31"

    for variant in GAME_IDS[game_key]:
        # Ejecutamos fetch dentro de la página para heredar cookies, referer, headers del navegador
        js = f"""
            const params = new URLSearchParams({{
              game_id: {json.dumps(variant)},
              fechaInicioInclusiva: {json.dumps(start)},
              fechaFinInclusiva:   {json.dumps(end)}
            }});
            const res = await fetch({json.dumps(BASE_JSON)} + "?" + params.toString(), {{
              method: "GET",
              credentials: "include",
              headers: {{
                "Accept": "application/json, text/plain, */*"
              }}
            }});
            if (!res.ok) {{
              return {{"ok": false, "status": res.status}};
            }}
            const data = await res.json();
            return {{"ok": true, "status": 200, "data": data}};
        """
        try:
            resp = page.evaluate(js)
            if not resp.get("ok"):
                print(f"[retry] {game_key} {year} ({variant}) --> HTTP {resp.get('status')}")
                jitter(0.8, 1.6)
                continue

            data = resp.get("data") or {}
            # la lista puede venir en varias llaves
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

            if parsed:
                print(f"[ok]  {game_key} {year} via '{variant}' -> {len(parsed)} sorteos")
                return parsed
            else:
                print(f"[warn] {game_key} {year} '{variant}' sin filas parseables; probando otra variante…")
                jitter()
        except Exception as e:
            print(f"[err] {game_key} {year} ({variant}): {e}")
            jitter(1.0, 2.0)

    print(f"[fail] {game_key} {year} -> sin datos tras variantes")
    return []


def run():
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")

    results = {k: [] for k in HOST_PAGE.keys()}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
        ])
        # Contexto “realista”
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
            timezone_id="Europe/Madrid",
            viewport={"width": 1366, "height": 900},
        )

        for game_key, url in HOST_PAGE.items():
            page = context.new_page()
            print(f"[run] {game_key} :: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # Aceptar cookies si aparece (inevitable en LAE)
            try:
                page.click("text=Aceptar", timeout=3_000)
            except:
                pass

            # Pequeña espera para que el site deposite cookies y tokens
            jitter(0.8, 1.4)

            for year in range(START_YEAR, END_YEAR + 1):
                rows = fetch_year_from_page(page, game_key, year)
                results[game_key].extend(rows)
                jitter(0.4, 0.9)

            page.close()

        context.close()
        browser.close()

    # Persistimos en /docs/api
    gen_at = datetime.utcnow().isoformat() + "Z"
    flat = sum(results.values(), [])

    payload = {
        "generated_at": gen_at,
        "results": flat,
        "by_game_counts": {k: len(v) for k, v in results.items()},
    }

    (OUT_DIR / "lae_historico.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Por juego
    for g, arr in results.items():
        (OUT_DIR / f"{g}.json").write_text(
            json.dumps({"generated_at": gen_at, "results": arr}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    # Latest (uno por juego)
    latest = latest_by_game(results)
    (OUT_DIR / "lae_latest.json").write_text(
        json.dumps({"generated_at": gen_at, "results": list(latest.values())}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])


if __name__ == "__main__":
    run()
