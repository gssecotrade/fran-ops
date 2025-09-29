# ops/scripts/fetch_lae_historic_browser.py
import os, json, time, random
from datetime import datetime, date
from playwright.sync_api import sync_playwright

OUT_DIR = os.path.join("docs", "api")
START_YEAR = 2020
END_YEAR   = date.today().year

GAMES = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
    "BONOLOTO" : "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
    "GORDO"    : "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
    "EURO"     : "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
}

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def scrape_game_year(page, game_key: str, url: str, year: int):
    results = []
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    # aceptar cookies si aparece
    try:
        page.locator("button:has-text('Aceptar')").first.click(timeout=3000)
    except:
        pass

    time.sleep(1.5)

    # ---- 1) Buscar en tarjetas ----
    cards = page.locator(".c-sorteo")
    if cards.count() > 0:
        for i in range(cards.count()):
            try:
                fecha = cards.nth(i).locator(".c-sorteo__fecha").inner_text().strip()
                comb = cards.nth(i).locator(".c-sorteo__combinacion").inner_text().strip()
                if str(year) not in fecha:
                    continue
                numeros = [int(x) for x in comb.replace("-", " ").replace(",", " ").split() if x.isdigit()]
                item = {"game": game_key, "date": fecha, "numbers": numeros[:6]}
                results.append(item)
            except Exception:
                continue

    # ---- 2) Buscar en tablas (backup) ----
    if not results:
        tables = page.locator("table")
        for i in range(min(2, tables.count())):
            try:
                rows = tables.nth(i).locator("tr")
                for j in range(rows.count()):
                    row = rows.nth(j).inner_text().strip()
                    if not row or str(year) not in row:
                        continue
                    tokens = row.replace("-", " ").replace(",", " ").split()
                    fecha = None
                    numeros = []
                    for tok in tokens:
                        if "/" in tok and tok.count("/") == 2:
                            fecha = tok
                        elif tok.isdigit():
                            numeros.append(int(tok))
                    if fecha and numeros:
                        results.append({"game": game_key, "date": fecha, "numbers": numeros[:6]})
            except:
                continue

    return results


def run():
    ensure_dir(OUT_DIR)
    print("=== LAE · HISTÓRICO (browser) · start ===")
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")

    master = {k: [] for k in GAMES.keys()}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = context.new_page()

        for game_key, url in GAMES.items():
            print(f"[run] {game_key} :: {url}")
            total_before = len(master[game_key])
            for y in range(START_YEAR, END_YEAR + 1):
                rows = scrape_game_year(page, game_key, url, y)
                if rows:
                    master[game_key].extend(rows)
                    print(f"[ok] {game_key} {y} -> {len(rows)} sorteos")
                else:
                    print(f"[warn] {game_key} {y} -> 0 sorteos")
                time.sleep(1 + random.uniform(0, 0.5))
            got = len(master[game_key]) - total_before
            print(f"[sum] {game_key} -> {got} sorteos (desde {START_YEAR})")

        context.close()
        browser.close()

    # guardar
    gen_at = datetime.utcnow().isoformat() + "Z"
    payload = {
        "generated_at": gen_at,
        "results": sum(master.values(), []),
        "by_game_counts": {k: len(v) for k, v in master.items()},
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    for g, arr in master.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": gen_at, "results": arr}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO (browser) · done ===")
    print("by_game_counts:", payload["by_game_counts"])


if __name__ == "__main__":
    run()
