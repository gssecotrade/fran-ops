# ops/scripts/fetch_lae_spider.py
import os, re, json, asyncio, math, sys
from datetime import datetime, date
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

OUT_API_DIR = Path("docs/api")
OUT_HTML_DIR = Path("docs/lae_html")   # para auditoría / ajuste de selectores
OUT_API_DIR.mkdir(parents=True, exist_ok=True)
OUT_HTML_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = 2020
END_YEAR = date.today().year

GAMES = {
    # nombre interno  :  (ruta indice sorteos, patrón dominio en href)
    "PRIMITIVA": ("https://www.loteriasyapuestas.es/es/la-primitiva/sorteos", r"/la-primitiva/"),
    "BONOLOTO" : ("https://www.loteriasyapuestas.es/es/bonoloto/sorteos", r"/bonoloto/"),
    "GORDO"    : ("https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos", r"/el-gordo-de-la-primitiva/"),
    "EURO"     : ("https://www.loteriasyapuestas.es/es/euromillones/sorteos", r"/euromillones/"),
}

# Selectores bastante genéricos (podemos afinarlos tras 1ª corrida con los HTML guardados)
SEL_CARD_LINKS = "a[href*='sorteos']"
SEL_LOAD_MORE  = "button:has-text('Cargar más'), button:has-text('Ver más'), a:has-text('Cargar más'), a:has-text('Ver más')"

# Extraer números: probamos varios patrones comunes
RE_NUM = re.compile(r"\b(\d{1,2})\b")
# “Complementario”, “Reintegro”, “Estrellas”, “Clave”
RE_COMP = re.compile(r"[Cc]omplementari[oa]\D+(\d{1,2})")
RE_REIN = re.compile(r"[Rr]eintegr[oa]\D+(\d)")
RE_CLAVE= re.compile(r"[Cc]lave\D+(\d{1,2})")
RE_EST  = re.compile(r"[Ee]strella[s]?\D+(\d{1,2})\D+(\d{1,2})")

def safe_write(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

async def scroll_or_load_more(page):
    """Intenta click en 'Cargar más' si existe; si no, hace scroll suave."""
    try:
        btn = page.locator(SEL_LOAD_MORE)
        if await btn.count() > 0:
            await btn.first.click(timeout=1500)
            await page.wait_for_timeout(700)
            return True
    except PWTimeout:
        pass
    # Scroll hasta abajo
    before = await page.evaluate("() => document.documentElement.scrollHeight")
    await page.mouse.wheel(0, 2500)
    await page.wait_for_timeout(700)
    after = await page.evaluate("() => document.documentElement.scrollHeight")
    return after > before

def extract_date_from_text(text: str) -> str | None:
    """
    Intenta sacar una fecha 'YYYY-MM-DD' del texto (o del href si lo incluimos).
    """
    # Formatos típicos ‘dd/mm/yyyy’, ‘dd-mm-yyyy’
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except Exception:
            return None
    # o ‘yyyy-mm-dd’
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def quick_parse_numbers(html: str, game_key: str):
    """
    Parser 'rápido' por expresiones regulares del HTML. Deja todo en campos opcionales
    y guarda lo que encuentre. Ajustaremos si hace falta tras ver los HTML.
    """
    # Quitar scripts para que RE_NUM no pille ids numéricos de JS
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", "", html, flags=re.S|re.I)
    # En Euromillones suelen aparecer 5+2; en Primitiva/Bonoloto 6 (+comp + reintegro).
    nums = [int(x) for x in RE_NUM.findall(cleaned)]
    # Heurística: limitamos a 15 primeros “candidatos” visibles
    nums = nums[:15]

    out = {}
    # Complementario/Reintegro/Clave/Estrellas
    m = RE_COMP.search(cleaned)
    if m: out["complementario"] = int(m.group(1))
    m = RE_REIN.search(cleaned)
    if m: out["reintegro"] = int(m.group(1))
    m = RE_CLAVE.search(cleaned)
    if m: out["clave"] = int(m.group(1))
    m = RE_EST.search(cleaned)
    if m: out["estrellas"] = [int(m.group(1)), int(m.group(2))]

    # Números principales: heurística por juego
    if game_key in ("PRIMITIVA", "BONOLOTO", "GORDO"):
        # coge los 6 más razonables (1..49/54)
        main = [n for n in nums if 1 <= n <= 60][:6]
        out["numbers"] = main
    else:  # EURO
        main = [n for n in nums if 1 <= n <= 70][:5]
        out["numbers"] = main
        if "estrellas" not in out:
            stars = [n for n in nums if 1 <= n <= 12]
            out["estrellas"] = stars[:2]
    return out

async def crawl_game(context, game_key: str, index_url: str, href_pattern: str):
    page = await context.new_page()
    await page.route("**/*", lambda route: route.continue_())  # no bloqueamos nada
    await page.goto(index_url, wait_until="domcontentloaded", timeout=60_000)

    print(f"[run] {game_key} :: {index_url}")

    # Descubrimos enlaces de sorteos hasta cubrir START_YEAR
    links = set()
    last_len = -1
    idle_rounds = 0

    for _ in range(80):  # cota de seguridad
        # capturamos todo enlace con patrón del juego + 'sorteos'
        anchors = page.locator(SEL_CARD_LINKS)
        count = await anchors.count()
        for i in range(count):
            href = await anchors.nth(i).get_attribute("href")
            if not href:
                continue
            if re.search(href_pattern, href):
                links.add(href)

        # ¿ya tenemos enlaces de 2020..hoy?
        # Si alguno lleva fecha en el propio href, úsala para decidir si seguir.
        years_seen = set()
        for href in links:
            m = re.search(r"(\d{4})", href)
            if m:
                years_seen.add(int(m.group(1)))
        if any(y <= START_YEAR for y in years_seen):
            break

        # si no crece el número de enlaces, contamos rondas “inactiva”
        if len(links) == last_len:
            idle_rounds += 1
        else:
            idle_rounds = 0
        last_len = len(links)

        moved = await scroll_or_load_more(page)
        if not moved:
            # sin scroll ni botón => paramos si llevamos varias rondas inactivas
            if idle_rounds >= 3:
                break

    print(f"[sum] {game_key} => {len(links)} enlaces candidatos")

    # Visitamos cada sorteo, guardamos HTML y parseamos
    results = []
    for href in sorted(links):
        url = href if href.startswith("http") else f"https://www.loteriasyapuestas.es{href}"
        try:
            p = await context.new_page()
            await p.goto(url, wait_until="domcontentloaded", timeout=60_000)
            # Por si la web tarda en pintar los números
            await p.wait_for_timeout(800)

            html = await p.content()
            # Guardamos html crudo para auditoría
            # Intentamos deducir fecha desde la propia página (title/h1) o el href
            text_all = await p.locator("body").inner_text()
            # 1) del texto
            dt = extract_date_from_text(text_all)
            # 2) del href (si trae yyyy)
            if not dt:
                m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", url)
                if m:
                    dt = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

            if not dt:
                # como fallback, usa título
                title = await p.title()
                dt = extract_date_from_text(title) or "0000-00-00"

            # guardado html
            safe_write(OUT_HTML_DIR / game_key / f"{dt}.html", html.encode("utf-8", "ignore"))

            parsed = quick_parse_numbers(html, game_key)
            parsed["game"] = game_key
            parsed["date"] = dt
            results.append(parsed)

            await p.close()
        except Exception as e:
            print(f"[warn] {game_key} fallo al abrir {url}: {e}")

    # Filtra rango de años & vacíos
    def in_range(d):
        try:
            y = int(d["date"][:4])
            return START_YEAR <= y <= END_YEAR
        except:
            return False

    results = [d for d in results if d.get("numbers") and in_range(d)]
    print(f"[sum] {game_key} -> {len(results)} sorteos útiles tras parseo")
    await page.close()
    return results

async def main():
    payload_all = {k: [] for k in GAMES}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"
        ])
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118.0 Safari/537.36"),
            viewport={"width": 1280, "height": 900},
            locale='es-ES'
        )

        print("=== LAE · HISTÓRICO (spider) · start ===")
        print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")

        for g, (idx_url, pat) in GAMES.items():
            arr = await crawl_game(context, g, idx_url, pat)
            payload_all[g] = arr

        await context.close()
        await browser.close()

    flat = []
    for arr in payload_all.values():
        flat.extend(arr)

    gen_at = datetime.utcnow().isoformat() + "Z"
    OUT_API_DIR.mkdir(parents=True, exist_ok=True)

    # maestro
    with open(OUT_API_DIR / "lae_historico.json", "w", encoding="utf-8") as f:
        json.dump({"generated_at": gen_at, "results": flat,
                   "by_game_counts": {k: len(v) for k, v in payload_all.items()}},
                  f, ensure_ascii=False, indent=2)

    # particionado
    for g, arr in payload_all.items():
        with open(OUT_API_DIR / f"{g}.json", "w", encoding="utf-8") as f:
            json.dump({"generated_at": gen_at, "results": arr}, f, ensure_ascii=False, indent=2)

    # latest por juego
    def dt_of(d):
        try:
            return datetime.strptime(d["date"], "%Y-%m-%d")
        except:
            return datetime.min

    latest = []
    for g, arr in payload_all.items():
        if arr:
            latest.append(sorted(arr, key=dt_of)[-1])
    with open(OUT_API_DIR / "lae_latest.json", "w", encoding="utf-8") as f:
        json.dump({"generated_at": gen_at, "results": latest}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (spider) · done ===")
    print("by_game_counts:", {k: len(v) for k, v in payload_all.items()})

if __name__ == "__main__":
    asyncio.run(main())
