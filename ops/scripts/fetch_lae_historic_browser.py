# ops/scripts/fetch_lae_historic_browser.py
# Histórico LAE 2020-hoy usando navegador real (Playwright)
# - Carga páginas oficiales /sorteos
# - Pulsa "Cargar más" hasta cubrir START_YEAR
# - Parseo robusto de filas (números, complementario, reintegro, estrellas)
import os, re, json, time, math, random
from datetime import datetime, date
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

OUT_DIR = os.path.join("docs", "api")
START_YEAR = int(os.environ.get("LAE_START_YEAR", "2020"))
END_YEAR   = date.today().year

GAMES = {
    # key           url path                               how to parse stars/complement/reintegro
    "PRIMITIVA": ("https://www.loteriasyapuestas.es/es/la-primitiva/sorteos", dict(has_comp=True, has_rein=True, has_stars=False)),
    "BONOLOTO":  ("https://www.loteriasyapuestas.es/es/bonoloto/sorteos",      dict(has_comp=True, has_rein=True, has_stars=False)),
    "GORDO":     ("https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos", dict(has_comp=False, has_rein=True, has_stars=False, has_clave=True)),
    "EURO":      ("https://www.loteriasyapuestas.es/es/euromillones/sorteos",  dict(has_comp=False, has_rein=False, has_stars=True)),
}

# Utilidades
def ensure_dir(p): os.makedirs(p, exist_ok=True)

NUM_RE = re.compile(r"\b\d{1,2}\b")
DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")

def parse_row_text(game_key: str, txt: str, flags: dict):
    """
    txt: texto visible de una fila de la tabla (una línea por sorteo)
    """
    # fecha
    m = DATE_RE.search(txt)
    if not m:
        return None
    fecha = m.group(1)  # dd/MM/yyyy

    # números “principales”: cogemos los primeros 6..7 números consecutivos,
    # evitando los que van después de palabras clave (Complementario, Reintegro, Estrellas...)
    # Estrategia simple: primero recolectamos todos los números en orden
    nums_all = [int(x) for x in NUM_RE.findall(txt)]
    # Luego intentamos identificar marcadores
    comp = None
    rein = None
    clave = None
    estrellas = []

    # Heurística por palabras clave (en español en el sitio oficial)
    # Intentamos localizar posiciones aproximadas
    comp_pos = txt.lower().find("complement") if flags.get("has_comp") else -1
    rein_pos = txt.lower().find("reinteg") if flags.get("has_rein") else -1
    est_pos  = txt.lower().find("estrella") if flags.get("has_stars") else -1
    clave_pos= txt.lower().find("clave") if flags.get("has_clave") else -1

    # Números principales: Euromillones tiene 5, resto 6
    main_count = 5 if flags.get("has_stars") else 6
    numbers = nums_all[:main_count] if len(nums_all) >= main_count else nums_all[:]

    # Extraer complementario / reintegro / estrellas / clave desde la cola
    tail = nums_all[main_count:]

    # Si hay clave (El Gordo), suele ser un número suelto
    if flags.get("has_clave") and len(tail) > 0:
        # lo último puede ser clave o reintegro; mantenemos orden: clave antes que reintegro
        clave = tail[0]
        tail = tail[1:]

    if flags.get("has_comp") and len(tail) > 0:
        comp = tail[0]
        tail = tail[1:]

    if flags.get("has_stars"):
        # dos estrellas
        if len(tail) >= 2:
            estrellas = tail[:2]
            tail = tail[2:]

    if flags.get("has_rein") and len(tail) > 0:
        rein = tail[0]
        tail = tail[1:]

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numbers,
    }
    if comp is not None: out["complementario"] = comp
    if rein is not None: out["reintegro"] = rein
    if clave is not None: out["clave"] = clave
    if estrellas: out["estrellas"] = estrellas
    return out

def click_load_more_until_year(page, min_year: int, max_clicks: int = 100):
    """
    Pulsa “Cargar más” (o similar) hasta que en la página haya sorteos <= min_year.
    Deja de pulsar si no aparece el botón o alcanzamos max_clicks.
    """
    for i in range(max_clicks):
        # ¿ya aparece un sorteo de min_year o anterior?
        content = page.content()
        years = re.findall(r"\b(\d{2}/\d{2}/(\d{4}))\b", content)
        if years:
            last_year = min(int(y[1]) for y in years)  # el más antiguo visible
            if last_year <= min_year:
                return
        # intenta localizar botones típicos
        clicked = False
        for sel in ['text="Cargar más"', 'text="Mostrar más"', 'button:has-text("más")', 'a:has-text("más")']:
            try:
                page.locator(sel).first.click(timeout=1500)
                page.wait_for_timeout(800)
                clicked = True
                break
            except Exception:
                pass
        if not clicked:
            # fin: no hay más
            return

def scrape_game(play, game_key: str, url: str, flags: dict):
    draws = []
    browser = play.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(locale="es-ES", user_agent=None)
    page = ctx.new_page()
    page.set_default_timeout(15000)

    try:
        page.goto(url, wait_until="networkidle")
        # aceptar cookies si aparece
        for sel in ['text="Aceptar"', 'text="Aceptar todo"', 'button:has-text("Aceptar")']:
            try:
                page.locator(sel).first.click(timeout=1500)
                page.wait_for_timeout(300)
                break
            except Exception:
                pass

        click_load_more_until_year(page, START_YEAR)

        # Extraer filas de la(s) tabla(s). Selección amplia:
        rows = page.locator("table >> tbody >> tr")
        count = rows.count()
        if count == 0:
            # algunos juegos muestran tarjetas; fallback: cada “sorteo” como item
            rows = page.locator("css=section article, div[role='article'], li")
            count = rows.count()

        for i in range(count):
            try:
                txt = rows.nth(i).inner_text().strip()
            except PWTimeout:
                continue
            if not txt:
                continue
            d = parse_row_text(game_key, txt, flags)
            if not d:
                continue
            # filtra por rango de fechas
            try:
                yy = int(d["date"][-4:])
                if yy < START_YEAR or yy > END_YEAR:
                    continue
            except:
                pass
            draws.append(d)

    finally:
        ctx.close()
        browser.close()

    # ordena por fecha asc
    def key_dt(x):
        try:
            return datetime.strptime(x["date"], "%d/%m/%Y")
        except:
            return datetime.strptime("01/01/1900", "%d/%m/%Y")
    draws.sort(key=key_dt)
    return draws

def latest_by_game(draws_by_game):
    res = {}
    for g, arr in draws_by_game.items():
        best = None
        best_dt = None
        for d in arr:
            try:
                dt = datetime.strptime(d["date"], "%d/%m/%Y")
            except:
                continue
            if best_dt is None or dt > best_dt:
                best_dt = dt
                best = d
        if best:
            res[g] = best
    return res

def main():
    print(f"=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")
    ensure_dir(OUT_DIR)
    all_draws = {k: [] for k in GAMES}

    with sync_playwright() as p:
        for game_key, (url, flags) in GAMES.items():
            print(f"[run] {game_key} :: {url}")
            try:
                arr = scrape_game(p, game_key, url, flags)
                all_draws[game_key] = arr
                print(f"[ok]  {game_key} -> {len(arr)} sorteos (desde {START_YEAR})")
            except Exception as e:
                print(f"[err] {game_key} -> {e}")

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_draws.values(), []),
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": payload["generated_at"], "results": arr}, f, ensure_ascii=False, indent=2)

    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": payload["generated_at"], "results": list(latest.values())}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
