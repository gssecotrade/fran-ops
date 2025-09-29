# ops/scripts/fetch_lae_historic.py
# Generador de histórico LAE (mejoras: cabeceras realistas + rotación UA + reintentos robustos)
import os, sys, json, time, math, random
from datetime import datetime, date
import requests

# ---------- Config ----------
OUT_DIR = os.path.join("docs", "api")
GAMES = {
    "PRIMITIVA": ["LA PRIMITIVA", "PRIMITIVA", "LAPRIMITIVA"],
    "BONOLOTO":  ["BONOLOTO"],
    "GORDO":     ["EL GORDO DE LA PRIMITIVA", "EL GORDO", "GORDO"],
    "EURO":      ["EUROMILLONES", "EURO MILLONES", "EURO"]
}

# Ajustado a petición: histórico desde 2020 (reduce tiempo y prob. de bloqueo)
START_YEAR = 2020
END_YEAR   = date.today().year

BASE = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# Lista de User-Agents (rotamos para simular varios navegadores reales)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)"
    " Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
]

# Cabeceras base que añadimos a las peticiones
BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.loteriasyapuestas.es/",
    "Origin": "https://www.loteriasyapuestas.es",
    "Connection": "keep-alive",
}

# Requests session; actual UA se asigna en cada intento
SESSION = requests.Session()
TIMEOUT = 30

def choose_user_agent():
    return random.choice(USER_AGENTS)

def retry_get(url, params, tries=6, base_sleep=0.8):
    """GET con reintentos robustos y rotación de UA. Lanza RuntimeError si no hay éxito."""
    last = None
    for i in range(1, tries+1):
        ua = choose_user_agent()
        headers = dict(BASE_HEADERS)
        headers["User-Agent"] = ua
        try:
            # hacemos la petición
            r = SESSION.get(url, params=params, headers=headers, timeout=TIMEOUT)
            code = r.status_code
            text = r.text or ""
            # si nos devuelven JSON
            if code == 200 and text.strip().startswith("{"):
                try:
                    return r.json()
                except Exception as e:
                    last = f"JSON parse error: {e}"
                    # dejar fallback a siguiente intento
            else:
                # status != 200 -> registro
                last = f"HTTP {code}"
                # si 403, esperar un backoff mayor antes de reintentar
                if code == 403:
                    sleep_time = base_sleep * (2 ** (i)) + random.uniform(0.5, 1.2)
                    print(f"[retry] 403 detected, sleeping {sleep_time:.1f}s (attempt {i}/{tries})")
                    time.sleep(sleep_time)
                    continue
        except Exception as e:
            last = str(e)
        # backoff exponencial + jitter
        sleep_time = base_sleep * (2 ** (i-1)) + random.uniform(0, 0.6)
        time.sleep(sleep_time)
    raise RuntimeError(f"Fallo GET JSON: {url}?{params} ({last})")

def normalize_draw(game_key, raw):
    """Convierte el objeto crudo LAE a nuestro formato común."""
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or "").strip()
    if not fecha:
        return None

    numeros = []
    comb = raw.get("combinacion") or raw.get("combinacionNumeros") or raw.get("numeros") or raw.get("bolas") or ""
    if isinstance(comb, str):
        parts = [p for p in comb.replace(",", " ").replace("-", " ").split() if p.strip()]
        for p in parts:
            try:
                numeros.append(int(p))
            except:
                pass
    elif isinstance(comb, list):
        for p in comb:
            try:
                numeros.append(int(p))
            except:
                pass

    complementario = raw.get("complementario")
    reintegro = raw.get("reintegro")
    clave = raw.get("clave")
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }
    if complementario is not None: out["complementario"] = complementario
    if reintegro is not None:     out["reintegro"] = reintegro
    if clave is not None:         out["clave"] = clave
    estrellas = []
    if e1 is not None: estrellas.append(e1)
    if e2 is not None: estrellas.append(e2)
    if estrellas: out["estrellas"] = estrellas
    return out

def fetch_year_for_variants(game_key, year, variants):
    start = f"{year}-01-01"
    end   = f"{year}-12-31"
    all_draws = []
    for idx, gid in enumerate(variants):
        params = {
            "game_id": gid,
            "fechaInicioInclusiva": start,
            "fechaFinInclusiva": end
        }
        try:
            data = retry_get(BASE, params)
            # LAE devuelve estructura con 'busqueda' / 'sorteos' / 'resultados'
            items = data.get("busqueda") or data.get("sorteos") or data.get("resultados") or data.get("buscador") or []
            if isinstance(items, dict):
                for v in items.values():
                    if isinstance(v, list):
                        items = v
                        break
            if not isinstance(items, list):
                items = []

            parsed = []
            for raw in items:
                d = normalize_draw(game_key, raw)
                if d and d.get("date"):
                    parsed.append(d)

            if parsed:
                print(f"[OK] {game_key} {year} via '{gid}' -> {len(parsed)} sorteos")
                all_draws.extend(parsed)
                break
            else:
                print(f"[warn] {game_key} {year} '{gid}' respondió sin sorteos")
        except Exception as e:
            print(f"[fail] {game_key} {year} ({gid}): {e}")
        # espera breve antes de próxima variante para no parecer bot
        time.sleep(0.8 + random.uniform(0, 0.6))
    return all_draws

def fetch_full_history():
    out = {k: [] for k in GAMES.keys()}
    for game_key, variants in GAMES.items():
        print(f"[cfg] Generando histórico para {game_key} ({START_YEAR}..{END_YEAR})")
        for year in range(START_YEAR, END_YEAR + 1):
            draws = fetch_year_for_variants(game_key, year, variants)
            out[game_key].extend(draws)
            # pausa corta entre años
            time.sleep(0.6 + random.uniform(0, 0.6))
    return out

def latest_by_game(draws):
    def parse_d(d):
        s = d["date"]
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        return None
    res = {}
    for g, arr in draws.items():
        best = None
        best_dt = None
        for d in arr:
            dt = parse_d(d)
            if dt and (best_dt is None or dt > best_dt):
                best_dt = dt
                best = d
        if best:
            res[g] = best
    return res

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def main():
    print("=== LAE · HISTÓRICO · start ===")
    ensure_dir(OUT_DIR)
    all_draws = fetch_full_history()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_draws.values(), []),
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": payload["generated_at"], "results": arr}, f, ensure_ascii=False, indent=2)

    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": payload["generated_at"], "results": list(latest.values())}, f, ensure_ascii=False, indent=2)

    print("=== LAE · HISTÓRICO · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
