# ops/scripts/fetch_lae_latest.py
import sys, json
from fetch_lae_common import get_html, parse_lotoideas_table, iso_now, write_json

# URLs corregidas (con '/' final)
URLS = {
    "PRIMITIVA": "https://www.lotoideas.com/historico-primitiva/",
    "BONOLOTO":  "https://www.lotoideas.com/historico-bonoloto/",
    "GORDO":     "https://www.lotoideas.com/historico-el-gordo-de-la-primitiva/",
    "EURO":      "https://www.lotoideas.com/historico-euromillones/",
}

def main(outfile):
    results = []
    errors = []

    for key, url in URLS.items():
        try:
            html = get_html(url)
            rows = parse_lotoideas_table(html, key)
            if rows:
                # el más reciente es la primera fila de la tabla
                results.append(rows[0])
            else:
                errors.append(f"{key}: sin filas parseadas")
        except Exception as e:
            errors.append(f"{key}: {e}")

    payload = {
        "generated_at": iso_now(),
        "results": results,
        "errors": errors,
    }
    write_json(payload, outfile)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_latest.json"
    main(out)
