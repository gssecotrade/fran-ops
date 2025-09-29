import sys
from fetch_lae_common import fetch_game, dump_payload

def main(outfile: str):
    errors = []
    results = []
    for g in ["PRIMITIVA", "BONOLOTO", "GORDO", "EURO"]:
        try:
            # Histórico: recorre varias páginas (ajusta si quieres más cobertura)
            page_results = fetch_game(g, max_pages=6)
            if page_results:
                results.extend(page_results)
            else:
                errors.append(f"{g}: no_data")
        except Exception as e:
            errors.append(f"{g}: {e.__class__.__name__}: {e}")
    # ordena por fecha descendente si vienen mezcladas
    def keyf(r):
        # dd/mm/aaaa -> aaaa-mm-dd para ordenar
        try:
            d, m, y = r["date"].split("/")
            return (int(y), int(m), int(d))
        except Exception:
            return (0,0,0)
    results.sort(key=keyf, reverse=True)
    dump_payload(outfile, results, errors)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "docs/api/lae_historico.json"
    main(out)
