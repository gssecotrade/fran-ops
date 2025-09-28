# ops/scripts/fetch_lae_historic.py
import sys
from urllib.parse import urljoin, urlencode

from fetch_lae_common import get_html, parse_lotoideas_table, iso_now, write_json

BASE = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva/",
    "BONOLOTO":  "https://www.lotoideas.com/historico-bonoloto/",
    "GORDO":     "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva/",
    "EURO":      "https://www.lotoideas.com/historico-euromillones/",
}

# De inicio 1 página por juego (rápido y estable). Para ampliar histórico, sube PAGES.
PAGES = 1

def page_url(base, page):
    # Lotoideas a veces usa “?pagina=N”; si no, “/page/N/”.
    # Probamos primero query param; si falla al parsear, el llamador añadirá el base “tal cual”.
    if page == 1:
        return base
    return base + f"?pagina={page}"

def main(outfile):
    all_results = []
    errors = []

    for key, base in BASE.items():
        merged = []
        for p in range(1, PAGES+1):
            url = page_url(base, p)
            try:
                html = get_html(url)
                rows = parse_lotoideas_table(html, key)
                if rows:
                    merged.extend(rows)
                else:
                    # si una página viene vacía, paramos paginado para ese juego
                    break
            except Exception as e:
                errors.append(f"{key} page {p}: {e}")
                break

        all_results.extend(merged)

    payload = {
        "generated_at": iso_now(),
        "results": all_results,
        "errors": errors,
    }
    write_json(payload, outfile)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_historico.json"
    main(out)
