#!/usr/bin/env python3
# ops/scripts/fetch_lae_results.py
# Scrapea resultados LAE con Playwright (headless) y actualiza Google Sheet.
import os, json, re, asyncio, base64
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

SHEET_ID = os.environ["CONTROL_SHEET_ID"]  # ENCRYPTED/secret en Actions
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SA = json.loads(base64.b64decode(os.environ["GOOGLE_SA_JSON_BASE64"]).decode("utf-8"))

# URLs de resultados (páginas públicas visibles en navegador)
FEEDS = {
    "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva",
    "BONOLOTO":  "https://www.loteriasyapuestas.es/es/bonoloto",
    "GORDO":     "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva",
    "EURO":      "https://www.loteriasyapuestas.es/es/euromillones",
}

# ===== Helpers Google Sheets =====
def open_sheet():
    creds = Credentials.from_service_account_info(SA, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

def header_map(sh):
    hdr = sh.row_values(1)
    return { (hdr[i] or "").strip().upper(): i+1 for i in range(len(hdr)) }

def upsert_by_fecha(sh, rowdict):
    idx = header_map(sh)
    if "FECHA" not in idx: raise RuntimeError(f"La hoja {sh.title} no tiene FECHA")
    fecha_col = idx["FECHA"]
    last = sh.row_count
    rng = sh.col_values(fecha_col)[1:]  # sin cabecera
    try:
        pos = 2 + rng.index(rowdict["FECHA"])
    except ValueError:
        pos = len(rng) + 2
    # prepara fila a longitud cabecera
    row = [""] * len(idx)
    for k, v in rowdict.items():
        c = idx.get(k.upper())
        if c: row[c-1] = v
    sh.update(f"A{pos}", [row], value_input_option="USER_ENTERED")

# ===== Parsers tolerantes sobre texto visible =====
MESES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12}
def pad2(n): return f"{int(n):02d}"

def parse_fecha(text):
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
    if m: return f"{pad2(m.group(1))}/{pad2(m.group(2))}/{m.group(3)}"
    m = re.search(r"(\d{1,2})\s*(?:de\s+)?([a-záéíóúñ]+)\s*(?:de\s+)?(\d{4})", text, re.I)
    if m:
        mes = re.sub(r"[^\w]", "", m.group(2).lower())
        mes = {"setiembre":"septiembre"}.get(mes, mes)
        if mes in MESES: return f"{pad2(m.group(1))}/{pad2(MESES[mes])}/{m.group(3)}"
    return None

def ints(text): return [int(x) for x in re.findall(r"\b\d{1,2}\b", text)]
def first_combo(nums, count, lo, hi):
    vals = [n for n in nums if lo <= n <= hi]
    return vals[:count] if len(vals) >= count else []

def parse_primitiva(text):
    FECHA = parse_fecha(text); 
    nums = ints(text)
    base = first_combo(nums, 6, 1, 49)
    if not FECHA or len(base)<6: return None
    tail = nums[nums.index(base[5])+1:] if base[5] in nums else []
    comp = next((n for n in tail if 1<=n<=49), "")
    rein = next((n for n in tail if 0<=n<=9), "")
    return {"FECHA":FECHA, "N1":base[0],"N2":base[1],"N3":base[2],"N4":base[3],"N5":base[4],"N6":base[5],
            "Complementario":comp, "Reintegro":rein}

def parse_bonoloto(text):
    FECHA = parse_fecha(text); 
    nums = ints(text)
    base = first_combo(nums, 6, 1, 49)
    if not FECHA or len(base)<6: return None
    tail = nums[nums.index(base[5])+1:] if base[5] in nums else []
    comp = next((n for n in tail if 1<=n<=49), "")
    rein = next((n for n in tail if 0<=n<=9), "")
    return {"FECHA":FECHA, "N1":base[0],"N2":base[1],"N3":base[2],"N4":base[3],"N5":base[4],"N6":base[5],
            "Complementario":comp, "Reintegro":rein}

def parse_gordo(text):
    FECHA = parse_fecha(text); 
    nums = ints(text)
    base = first_combo(nums, 5, 1, 54)
    if not FECHA or len(base)<5: return None
    tail = nums[nums.index(base[4])+1:] if base[4] in nums else []
    clave = next((n for n in tail if 0<=n<=9), "")
    return {"FECHA":FECHA, "N1":base[0],"N2":base[1],"N3":base[2],"N4":base[3],"N5":base[4], "Clave":clave}

def parse_euro(text):
    FECHA = parse_fecha(text); 
    nums = ints(text)
    base = first_combo(nums, 5, 1, 50)
    if not FECHA or len(base)<5: return None
    tail = nums[nums.index(base[4])+1:] if base[4] in nums else []
    est = [n for n in tail if 1<=n<=12][:2]
    if len(est)<2:
        est = [n for n in nums if 1<=n<=12 and n not in base][:2]
        if len(est)<2: return None
    return {"FECHA":FECHA, "N1":base[0],"N2":base[1],"N3":base[2],"N4":base[3],"N5":base[4], "E1":est[0], "E2":est[1]}

PARSERS = {
    "PRIMITIVA": ("Historico",      parse_primitiva),
    "BONOLOTO":  ("HistoricoBono",  parse_bonoloto),
    "GORDO":     ("HistoricoGordo", parse_gordo),
    "EURO":      ("HistoricoEuro",  parse_euro),
}

async def grab_text(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                                        locale="es-ES")
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # Espera corta a que pinten módulos
        await page.wait_for_timeout(2000)
        txt = await page.locator("body").inner_text()
        await browser.close()
        return txt

async def main_async():
    ss = open_sheet()
    for game, url in FEEDS.items():
        print(f"→ {game} :: {url}")
        text = await grab_text(url)
        sheet_name, parser = PARSERS[game]
        row = parser(text)
        if not row:
            print(f"  ⚠️  {game}: no se pudo parsear")
            continue
        upsert_by_fecha(ss.worksheet(sheet_name), row)
        print(f"  ✓ {game}: {row['FECHA']} -> actualizado")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
