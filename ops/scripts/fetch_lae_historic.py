import sys, json, requests
from bs4 import BeautifulSoup
from datetime import datetime

def fetch_game(game_id, url):
    print(f"[fetch] {game_id} -> {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = []

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("table tr")  # ajustar según estructura real
    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if not cols: continue
        try:
            date = cols[0]
            numbers = [int(x) for x in cols[1:7] if x.isdigit()]
            extra = cols[7:] if len(cols) > 7 else []
            draw = {
                "game": game_id,
                "date": date,
                "numbers": numbers,
            }
            if extra:
                draw["extra"] = extra
            data.append(draw)
        except Exception as e:
            print(f"[warn] {game_id} row skipped: {e}")
    return data

def main(outfile):
    # URLs oficiales o mirror scraping (placeholder ahora)
    GAMES = {
        "PRIMITIVA": "https://www.loteriasyapuestas.es/es/la-primitiva/sorteos",
        "BONOLOTO": "https://www.loteriasyapuestas.es/es/bonoloto/sorteos",
        "GORDO": "https://www.loteriasyapuestas.es/es/el-gordo-de-la-primitiva/sorteos",
        "EURO": "https://www.loteriasyapuestas.es/es/euromillones/sorteos",
    }

    all_results = []
    for game, url in GAMES.items():
        try:
            all_results.extend(fetch_game(game, url))
        except Exception as e:
            print(f"[error] {game}: {e}")

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": all_results,
    }

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[done] Escrito {len(all_results)} sorteos en {outfile}")

if __name__ == "__main__":
    outfile = sys.argv[1] if len(sys.argv) > 1 else "lae_historico.json"
    main(outfile)
