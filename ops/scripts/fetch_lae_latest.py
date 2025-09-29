import sys
from fetch_lae_common import fetch_game, dump_payload

def main(outfile: str):
    errors = []
    results = []
    for g in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        try:
            # Sólo primera página (MAX_PAGES=1) para "latest"
            page_results = fetch_game(g, max_pages=1)
            if page_results:
                # coge los 1-2 sorteos más recientes por seguridad
                results.extend(page_results[:2])
            else:
                errors.append(f"{g}: no_data")
        except Exception as e:
            errors.append(f"{g}: {e.__class__.__name__}: {e}")
    dump_payload(outfile, results, errors)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_latest.json"
    main(out)
