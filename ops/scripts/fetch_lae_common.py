import re, json, time
from datetime import datetime
from typing import List, Dict, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Slug de cada juego en lotoideas
SOURCES = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva/",
    "BONOLOTO": "https://www.lotoideas.com/historico-bonoloto/",
    "GORDO": "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva/",
    "EURO": "https://www.lotoideas.com/historico-euromillones/",
}

def _strip(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def try_accept_cookies(page) -> None:
    """Intenta cerrar diferentes banners de cookies comunes."""
    selectors = [
        # didomi / oneTrust / gdpr genéricos
        'button:has-text("ACEPTAR")', 'button:has-text("Aceptar")',
        'button:has-text("Acepto")', 'button:has-text("I agree")',
        '#didomi-notice-agree-button', '.fc-cta-consent', 'button[aria-label*="Aceptar"]',
        'button[aria-label*="aceptar"]', 'button[title*="Aceptar"]',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=1500):
                el.click(timeout=1500)
                time.sleep(0.2)
                break
        except Exception:
            pass

def open_with_fallback(page, url: str) -> bool:
    """Carga url y comprueba que no sea 404. Devuelve True si OK."""
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if resp and 200 <= resp.status < 400:
            return True
    except Exception:
        pass
    # intento alternativo de paginación tipo WP: /page/1/
    try:
        alt = url.rstrip("/") + "/page/1/"
        resp = page.goto(alt, wait_until="domcontentloaded", timeout=60000)
        if resp and 200 <= resp.status < 400:
            return True
    except Exception:
        pass
    return False

def get_first_table(page):
    table = page.locator("table").first
    table.wait_for(state="visible", timeout=30000)
    return table

def parse_numbers(cell_text: str) -> List[int]:
    # Normaliza "01 08 13 36 40 45" o con comas
    nums = re.findall(r"\d{1,2}", cell_text or "")
    out = []
    for n in nums:
        try:
            out.append(int(n))
        except Exception:
            pass
    return out

def normalize_row(game: str, cols: List[str]) -> Dict:
    """
    Lotoideas suele tener columnas:
    FECHA | COMB. GANADORA | COMP. | R.   (PRIMITIVA / BONOLOTO)
    FECHA | COMB. GANADORA | CLAVE (GORDO)
    FECHA | COMB. GANADORA | ESTRELLAS (EURO)
    """
    row = {}
    if not cols:
        return row
    fecha = _strip(cols[0])
    row["date"] = fecha  # dd/mm/aaaa (la hoja ya espera este formato)
    comb = parse_numbers(cols[1] if len(cols) > 1 else "")
    if game in ("PRIMITIVA", "BONOLOTO"):
        row["numbers"] = comb[:6]
        row["complementario"] = parse_numbers(cols[2] if len(cols) > 2 else "")[:1][0] if len(cols) > 2 else None
        row["reintegro"] = parse_numbers(cols[3] if len(cols) > 3 else "")[:1][0] if len(cols) > 3 else None
    elif game == "GORDO":
        row["numbers"] = comb[:5]
        row["clave"] = parse_numbers(cols[2] if len(cols) > 2 else "")[:1][0] if len(cols) > 2 else None
    elif game == "EURO":
        row["numbers"] = comb[:5]
        est = parse_numbers(cols[2] if len(cols) > 2 else "")
        row["estrellas"] = est[:2]
    row["game"] = game
    row["source"] = "lotoideas"
    return row

def scrape_page_table(page, game: str) -> List[Dict]:
    table = get_first_table(page)
    trs = table.locator("tbody tr")
    n = trs.count()
    out: List[Dict] = []
    for i in range(n):
        cells = trs.nth(i).locator("td")
        ccount = cells.count()
        cols = [ _strip(cells.nth(j).inner_text()) for j in range(ccount) ]
        # filtra filas vacías o cabeceras copiadas
        if not cols or "FECHA" in cols[0].upper():
            continue
        row = normalize_row(game, cols)
        # descarta si no trae fecha o numbers
        if row.get("date") and row.get("numbers"):
            out.append(row)
    return out

def build_page_urls(base: str, max_pages: int) -> List[str]:
    base = base.rstrip("/")
    urls = [base + "/"]  # portada
    # paginación wordpress típica
    for p in range(2, max_pages + 1):
        urls.append(f"{base}/page/{p}/")
    return urls

def fetch_game(game: str, max_pages: int = 3) -> List[Dict]:
    base = SOURCES[game]
    results: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-gpu"])
        ctx = browser.new_context(user_agent=UA, locale="es-ES")
        page = ctx.new_page()
        try:
            for url in build_page_urls(base, max_pages):
                if not open_with_fallback(page, url):
                    continue
                try_accept_cookies(page)
                # Si la tabla no aparece, salta a la siguiente página
                try:
                    page_results = scrape_page_table(page, game)
                except PWTimeoutError:
                    page_results = []
                if not page_results:
                    # no hay datos en esta página -> pasamos a la siguiente
                    continue
                results.extend(page_results)
        finally:
            ctx.close()
            browser.close()
    return results

def dump_payload(path: str, results: List[Dict], errors: List[str]) -> None:
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": results,
        "errors": errors,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
