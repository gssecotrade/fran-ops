import time, json, re, sys, random
from datetime import datetime
import requests
from bs4 import BeautifulSoup

UA_POOL = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
]

def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language":"es-ES,es;q=0.9",
        "Cache-Control":"no-cache",
        "Pragma":"no-cache",
        "DNT":"1",
        "Upgrade-Insecure-Requests":"1",
    })
    s.timeout = 30
    return s

def _get_html(s, url, tries=4, wait=1.2):
    last = None
    for i in range(tries):
        try:
            r = s.get(url, timeout=30)
            if r.status_code == 200 and "<html" in r.text.lower():
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(wait * (i+1))
        # rota UA para burlar filtros simples
        s.headers["User-Agent"] = random.choice(UA_POOL)
    raise RuntimeError(f"Falló GET {url}: {last}")

def _parse_date(s):
    """
    Admite 'dd/mm/yyyy', 'dd-mm-yyyy' o 'yyyy-mm-dd'
    """
    s = s.strip()
    m = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', s)
    if m:  # dd/mm/yyyy
        d, M, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{M}-{d}"
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return s

def _ints(cells):
    out = []
    for c in cells:
        m = re.findall(r'\d+', c)
        if not m: out.append(None)
        else: out.append(int(m[0]))
    return out

def scrape_lotoideas_table(html, game):
    """
    Lotoideas presenta una tabla con cabecera:
      FECHA | COMB. GANADORA | COMP. | R.
    Extraemos fecha y 6 números + extra (compl/reintegro) si existiera.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No se encontró tabla en el HTML")

    rows = table.find_all("tr")
    data = []
    for tr in rows[1:]:
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 2:  # fila vacía
            continue
        fecha = _parse_date(tds[0])

        # La “COMB. GANADORA” viene como 6 números separados por espacio
        comb_txt = tds[1]
        nums = _ints(re.findall(r'\d+', comb_txt))[:6]
        if len(nums) < 5:   # fila inválida
            continue

        compl = None
        rein = None
        # algunas páginas incluyen columnas 'COMP.' y 'R.'
        if len(tds) >= 3:
            try: compl = int(re.findall(r'\d+', tds[2])[0])
            except: pass
        if len(tds) >= 4:
            try: rein = int(re.findall(r'\d+', tds[3])[0])
            except: pass

        entry = {
            "game": game,
            "date": fecha,
            "numbers": nums[:6],
        }
        # reglas por juego
        if game in ("PRIMITIVA","BONOLOTO"):
            entry["complementario"] = compl
            entry["reintegro"] = rein
        elif game == "GORDO":
            # Gordo suele tener 5 + clave
            if len(nums) >= 5:
                entry["numbers"] = nums[:5]
            entry["clave"] = compl if compl is not None else rein
        elif game == "EURO":
            # euromillones: 5+2 (estrellas). Usamos comp/rein como fallback
            entry["numbers"] = nums[:5]
            stars = []
            if compl is not None: stars.append(compl)
            if rein  is not None: stars.append(rein)
            entry["estrellas"] = stars[:2]
        data.append(entry)
    return data

def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
