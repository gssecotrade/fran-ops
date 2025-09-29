# ops/scripts/fetch_lae_historic_browser.py
import os, json, time, random
from datetime import datetime, date
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

# -------- Config ----------
OUT_DIR = os.path.join("docs", "api")
START_YEAR = 2020                         # últimos 5 años (ajústalo si quieres)
END_YEAR   = date.today().year

GAMES = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO" : "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO"    : "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO"     : "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def parse_rows_from_table(table_html: str, game_key: str):
    """
    Recibe outerHTML de la tabla y devuelve lista de {date, numbers, ...}
    La tabla suele tener fecha en la 1ª col y la combinación en una col tipo '01 02 03 ...'
    """
    # Parse muy defensivo (sin bs4 para no añadir deps): split por <tr>
    out = []
    trs = table_html.split("<tr")
    for tr in trs[1:]:
        # fecha (buscamos patrón dd/mm/yyyy)
        # números: secuencia de tokens 1–2 dígitos
        # reintegro/complementario puede ir en celdas extras
        row_html = tr.lower()
        # fecha
        fecha = None
        for tok in row_html.replace("<", " ").replace(">", " ").split():
            if "/" in tok and tok.count("/") == 2:
                # ej. 26/09/2025
                parts = tok.split("/")
                if len(parts) == 3 and all(p.isdigit() for p in parts):
                    d, m, y = parts
                    if len(y) == 4:
                        fecha = f"{d.zfill(2)}/{m.zfill(2)}/{y}"
                        break
        # números: juntamos todos los tokens de 1-2 dígitos consecutivos (máx 6)
        nums = []
        tokens = row_html.replace("&nbsp;", " ").replace("<", " ").replace(">", " ").split()
        for t in tokens:
            if t.isdigit() and 1 <= len(t) <= 2:
                try:
                    v = int(t)
                except:
                    continue
                # límites razonables según juego
                if 1 <= v <= 60:
                    nums.append(v)
        # recortar a (como máximo) 6 números para PRIMITIVA/BONOLOTO/GORDO.
        # para EURO luego detectamos estrellas (se suelen mostrar fuera del rango 1..60)
        estrellas = []
        if game_key == "EURO":
            # heurística: los 5 primeros números y luego dos estrellas máx 12
            core = []
            rest = []
            for v in nums:
                if v <= 50:
                    core.append(v)
                else:
                    rest.append(v)
            nums = core[:5]
            estrellas = [v for v in rest if 1 <= v <= 12][:2]
        else:
            nums = nums[:6]

        if fecha and nums:
            item = {"game": game_key, "date": fecha, "numbers": nums}
            if estrellas:
                item["estrellas"] = estrellas
            out.append(item)
    return out


def scrape_game_year(page, game_key: str, url: str, year: int):
    """
    Navega a la página de sorteos del juego y extrae filas del año indicado.
    No usamos `await` dentro de `evaluate`; hacemos waits por selectores con Playwright.
    """
    results = []
    page.route("**/*", lambda route: route.continue_())
    page.set_extra_http_headers({"User-Agent": UA})
    # La web carga filtro de año por interfaz; si hay selector, lo usamos;
    # si no, sacamos todas las filas visibles y filtramos por fecha regex.
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    # A veces hay overlays/cookies; intentamos cerrarlos de forma segura
    try:
        # botón de aceptar cookies frecuente
        btns = page.locator("button:has-text('Aceptar'), button:has-text('ACEPTAR')")
        if btns.count() > 0:
            btns.first.click(timeout=3000)
    except Exception:
        pass

    # Espera a que aparezca alguna tabla
    try:
        page.wait_for_selector("table", timeout=20000)
    except PwTimeout:
        return results  # sin tabla → año sin datos visible

    # Si existe un desplegable de año, lo intentamos (no todas las páginas lo tienen)
    try:
        sel = page.locator("select, [role='combobox']")
        if sel.count() > 0:
            # probamos varias opciones que suelan contener el año
            found = False
            all_opts = sel.first.locator("option")
            if all_opts.count() > 0:
                texts = [all_opts.nth(i).inner_text().strip() for i in range(all_opts.count())]
                for i, t in enumerate(texts):
                    if str(year) in t:
                        all_opts.nth(i).click()
                        found = True
                        break
            if found:
                # espera a que la tabla refresque
                time.sleep(1.2)
    except Exception:
        pass

    # Ahora leemos las tablas que estén en el DOM
    tables = page.locator("table")
    for i in range(min(3, tables.count())):  # por si hay varias, nos quedamos con las 3 primeras
        try:
            outer = tables.nth(i).evaluate("el => el.outerHTML")
        except Exception:
            continue
        rows = parse_rows_from_table(outer, game_key)
        # filtramos por año (dd/mm/yyyy)
        rows = [r for r in rows if r.get("date", "").endswith(str(year))]
        results.extend(rows)

    # deduplicado por fecha
    seen = set()
    dedup = []
    for r in results:
        k = r["date"]
        if k not in seen:
            seen.add(k)
            dedup.append(r)
    return dedup


def run():
    ensure_dir(OUT_DIR)
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")

    master = {k: [] for k in GAMES.keys()}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            user_agent=UA,
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True,
        )
        page = context.new_page()

        for game_key, url in GAMES.items():
            print(f"[run] {game_key} :: {url}")
            total_before = len(master[game_key])
            for y in range(START_YEAR, END_YEAR + 1):
                for attempt in range(1, 3):  # un par de reintentos suaves por año
                    try:
                        rows = scrape_game_year(page, game_key, url, y)
                        if rows:
                            master[game_key].extend(rows)
                            print(f"[ok] {game_key} {y} -> {len(rows)} sorteos")
                        else:
                            print(f"[warn] {game_key} {y} -> 0 sorteos")
                        break
                    except Exception as e:
                        print(f"[err] {game_key} {y}: {e} (attempt {attempt}/2)")
                        time.sleep(0.8 + random.uniform(0, 0.6))
                # pequeño respiro entre años para no parecer bot
                time.sleep(0.6 + random.uniform(0, 0.6))
            got = len(master[game_key]) - total_before
            print(f"[sum] {game_key} -> {got} sorteos (desde {START_YEAR})")

        context.close()
        browser.close()

    # persistimos
    gen_at = datetime.utcnow().isoformat() + "Z"
    flat = sum(master.values(), [])
    payload = {
        "generated_at": gen_at,
        "results": flat,
        "by_game_counts": {k: len(v) for k, v in master.items()},
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # por juego
    for g, arr in master.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": gen_at, "results": arr}, f, ensure_ascii=False, indent=2)

    # latest (por juego, la fecha más reciente)
    def ddmmyyyy_to_key(s):
        # "dd/mm/yyyy" → (yyyy,mm,dd)
        try:
            d, m, y = s.split("/")
            return (int(y), int(m), int(d))
        except:
            return (0, 0, 0)

    latest = []
    for g, arr in master.items():
        arr2 = sorted(arr, key=lambda r: ddmmyyyy_to_key(r["date"]), reverse=True)
        if arr2:
            latest.append(arr2[0])

    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": gen_at, "results": latest}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])


if __name__ == "__main__":
    run()
