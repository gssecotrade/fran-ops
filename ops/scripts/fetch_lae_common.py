# ops/scripts/fetch_lae_common.py
import time, json, re
from datetime import datetime
import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

def get_html(url, tries=4, backoff=0.8):
    last = None
    for i in range(tries):
        try:
            r = SESSION.get(url, timeout=30)
            if r.status_code == 200 and r.text.strip():
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(backoff*(i+1))
    raise RuntimeError(f"Falló GET {url}: {last}")

def parse_lotoideas_table(html, game_key):
    """
    Lotoideas pinta una tabla con cabeceras:
      FECHA | COMB. GANADORA (6 columnas) | COMP. | R.
    Devolvemos una lista de dicts uniformes con:
      date (yyyy-mm-dd), numbers[6], complementario, reintegro
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No se pudo obtener HTML válido (sin <table>)")

    out = []
    rows = table.find_all("tr")
    # saltamos cabecera
    for tr in rows[1:]:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds:
            continue

        # Esperamos ~ 1 + 6 + 1 + 1 = 9 columnas
        try:
            fecha = tds[0]
            # normalizamos fecha dd/mm/yyyy o dd-mm-yyyy
            fecha = fecha.replace("-", "/")
            d = datetime.strptime(fecha, "%d/%m/%Y").date()

            comb = []
            # columnas 1..6 (6 bolas)
            for x in tds[1:7]:
                m = re.search(r"\d+", x or "")
                comb.append(int(m.group(0)) if m else None)
            # complementario (índice 7)
            comp = int(re.search(r"\d+", tds[7]).group(0)) if len(tds) > 7 and re.search(r"\d+", tds[7]) else None
            # reintegro (índice 8)
            rein = int(re.search(r"\d+", tds[8]).group(0)) if len(tds) > 8 and re.search(r"\d+", tds[8]) else None

            out.append({
                "game": game_key,
                "date": d.isoformat(),
                "numbers": comb[:6],
                "complementario": comp,
                "reintegro": rein,
                "source": "lotoideas",
            })
        except Exception:
            # fila que no cumple, la ignoramos
            continue
    return out

def iso_now():
    return datetime.utcnow().isoformat() + "Z"

def write_json(payload, outfile):
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
