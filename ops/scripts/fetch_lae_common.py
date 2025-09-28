       #!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, time, pathlib
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

ROOT = pathlib.Path(__file__).resolve().parents[2]
API_DIR = ROOT / "docs" / "api"
API_DIR.mkdir(parents=True, exist_ok=True)

SOURCES: Dict[str, str] = {
    "PRIMITIVA": "https://www.resultadosloterias.es/la-primitiva/sorteos",
    "BONOLOTO":  "https://www.resultadosloterias.es/bonoloto/sorteos",
    "GORDO":     "https://www.resultadosloterias.es/el-gordo-primitiva/sorteos",
    "EURO":      "https://www.resultadosloterias.es/euromillones/sorteos",
}
DEFAULT_MAX_PAGES = 10

COOKIE_TEXTS = ["Aceptar","Acepto","Consentir","De acuerdo","I agree","Accept","Allow all","Agree"]
RE_NUM  = re.compile(r"\d+")
RE_DATE = re.compile(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})")

def utcnow_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def norm_date(s: str) -> str:
    s = s.strip()
    m = RE_DATE.search(s)
    if not m:
        try:
            return str(datetime.fromisoformat(s).date())
        except Exception:
            return ""
    d, mth, y = m.group(1), m.group(2), m.group(3)
    if len(y)==2: y = "20"+y
    return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"

def num_list_from_cells(cells: List[str]) -> List[int]:
    vals: List[int] = []
    for c in cells:
        for x in RE_NUM.findall(c):
            vals.append(int(x))
    return vals

def accept_cookies(page) -> None:
    for txt in COOKIE_TEXTS:
        try:
            page.get_by_role("button", name=txt, exact=False).click(timeout=1500)
            time.sleep(0.2); return
        except Exception:
            pass
    try:
        page.locator("button:has-text('cookie'), button:has-text('Cookie')").first.click(timeout=1500)
    except Exception:
        pass

def goto_and_ready(page, url: str):
    page.set_default_timeout(15_000)
    page.goto(url, wait_until="domcontentloaded")
    accept_cookies(page)
    try:
        page.wait_for_selector("table", state="visible", timeout=15_000)
    except PWTimeout:
        pass

def extract_rows_from_table(page) -> List[List[str]]:
    rows: List[List[str]] = []
    tables = page.locator("table")
    n = tables.count()
    for i in range(n):
        t = tables.nth(i)
        rcount = t.locator("tr").count()
        if rcount < 6:  # evita tablas de maquetación
            continue
        for r in range(1, rcount):
            row = t.locator("tr").nth(r)
            cells_loc = row.locator("th,td")
            cc = cells_loc.count()
            cells = [cells_loc.nth(c).inner_text().strip() for c in range(cc)]
            if any(cells):
                rows.append(cells)
    return rows

def parse_draw(game: str, cells: List[str]) -> Dict:
    if not cells: return {}
    date = norm_date(cells[0])
    nums = num_list_from_cells(cells[1:])
    if not date or not nums: return {}
    out: Dict = {"game": game, "date": date, "numbers": []}

    if game in ("PRIMITIVA","BONOLOTO"):
        out["numbers"] = nums[:6] if len(nums)>=6 else nums
        if len(nums)>=7: out["complementario"] = nums[6]
        if len(nums)>=8: out["reintegro"]      = nums[7]

    elif game=="GORDO":
        out["numbers"] = nums[:5] if len(nums)>=5 else nums
        if len(nums)>=6: out["clave"] = nums[5]

    elif game=="EURO":
        out["numbers"] = nums[:5] if len(nums)>=5 else nums
        estrellas = nums[5:7] if len(nums)>=7 else nums[5:]
        if estrellas: out["estrellas"] = estrellas[:2]
    else:
        out["numbers"] = nums
    return out

def next_page(page) -> bool:
    selectors = [
        "a[rel='next']","a:has-text('Siguiente')","a:has-text('Next')",
        "button:has-text('Siguiente')",".pagination a[rel='next']",
        "a[aria-label='Next']",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count()>0 and loc.is_enabled():
                loc.click(); page.wait_for_timeout(400); return True
        except Exception:
            pass
    return False

def scrape_game(browser, game: str, url: str, max_pages: int) -> List[Dict]:
    page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36")
    results: List[Dict] = []
    try:
        goto_and_ready(page, url)
        pages = 0
        while True:
            rows = extract_rows_from_table(page)
            for cells in rows:
                d = parse_draw(game, cells)
                if d: results.append(d)
            pages += 1
            if pages >= max_pages: break
            if not next_page(page): break
    finally:
        page.close()
    return results

def scrape_all(max_pages: int = DEFAULT_MAX_PAGES) -> Dict[str, List[Dict]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        all_data: Dict[str, List[Dict]] = {}
        for game, url in SOURCES.items():
            try:
                draws = scrape_game(browser, game, url, max_pages=max_pages)
                draws.sort(key=lambda x: x.get("date",""))
                all_data[game] = draws
            except Exception:
                all_data[game] = []
        browser.close()
    return all_data

def write_json(path: pathlib.Path, payload: dict) -> None:
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def build_payload_latest(all_data: Dict[str, List[Dict]]) -> dict:
    res = []
    for g in ("PRIMITIVA","BONOLOTO","GORDO","EURO"):
        lst = all_data.get(g, [])
        res.append(lst[-1] if lst else {"game": g, "date": "", "numbers": []})
    return {"generated_at": utcnow_z(), "results": res, "errors": []}

def build_payload_historic(all_data: Dict[str, List[Dict]]) -> dict:
    flat: List[Dict] = []
    for g in ("PRIMITIVA","BONOLOTO","GORDO","EURO"):
        flat.extend(all_data.get(g, []))
    flat.sort(key=lambda x: x.get("date",""))
    return {"generated_at": utcnow_z(), "results": flat, "errors": []}
