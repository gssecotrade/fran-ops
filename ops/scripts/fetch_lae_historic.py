# ops/scripts/fetch_lae_historic.py
# Scraping real (headless) de Lotoideas para construir docs/api/lae_historico.json

from __future__ import annotations
import asyncio
from pathlib import Path
from typing import List, Tuple, Dict, Any

from playwright.async_api import async_playwright

from fetch_lae_common import (
    today_utc,
    write_json,
    pack_primitiva_bonoloto,
    pack_gordo,
    pack_euro,
)

# --- Config ---
URLS = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva/",
    "BONOLOTO":  "https://www.lotoideas.com/historico-bonoloto/",
    "GORDO":     "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva/",
    "EURO":      "https://www.lotoideas.com/historico-euromillones/",
}
MAX_PAGES = 15  # sube/baja según profundidad deseada
OUT = Path("docs/api/lae_historico.json")
OUT.parent.mkdir(parents=True, exist_ok=True)


# --- Scraper genérico de tabla ---
async def scrape_table(page) -> List[List[str]]:
    await page.wait_for_selector("table", timeout=30000)
    rows = await page.query_selector_all("table tr")
    data: List[List[str]] = []
    for i, row in enumerate(rows):
        if i == 0:
            continue  # cabecera
        cells = await row.query_selector_all("td")
        vals = [(await c.inner_text()).strip() for c in cells]
        if any(v.strip() for v in vals):
            data.append(vals)
    return data


async def fetch_game(browser, game: str, base_url: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    ctx = await browser.new_context()
    page = await ctx.new_page()
    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for p in range(1, MAX_PAGES + 1):
        url = base_url if p == 1 else f"{base_url}?_page={p}"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            rows = await scrape_table(page)
            if not rows:
                break

            for r in rows:
                if game in ("PRIMITIVA", "BONOLOTO"):
                    packed = pack_primitiva_bonoloto(r, game)
                elif game == "GORDO":
                    packed = pack_gordo(r)
                else:
                    packed = pack_euro(r)

                if packed:
                    results.append(packed)
        except Exception as e:
            errors.append(f"{game} page {p}: {e}")
            break

    await ctx.close()
    return results, errors


async def main():
    payload = {"generated_at": today_utc(), "results": [], "errors": []}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        all_results: List[Dict[str, Any]] = []
        all_errors: List[str] = []

        for game, url in URLS.items():
            res, errs = await fetch_game(browser, game, url)
            all_results.extend(res)
            all_errors.extend(errs)

        await browser.close()

    payload["results"] = all_results
    payload["errors"] = all_errors

    write_json(str(OUT), payload)


if __name__ == "__main__":
    asyncio.run(main())
