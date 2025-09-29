# ops/scripts/fetch_lae_historic.py
import os, sys, json, time, random
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

# Rango de años (por defecto: desde 2020 al año actual).
# Se puede sobrescribir con variables de entorno LAE_START_YEAR y LAE_END_YEAR
def _int_env(name: str, default: int) -> int:
    try:
        v = os.getenv(name, "")
        return int(v) if v.strip() else default
    except Exception:
        return default

DEFAULT_START = 2020
START_YEAR = _int_env("LAE_START_YEAR", DEFAULT_START)
END_YEAR   = _int_env("LAE_END_YEAR", date.today().year)

# Endpoint oficial
BASE = "https://www.loteriasyapuestas.es/servicios/buscadorSorteos"

# Cabeceras realistas (ayuda a reducir 403)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.loteriasyapuestas.es/",
    "Origin": "https://www.loteriasyapuestas.es",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
TIMEOUT = 25

def retry_get(url, params, tries=5, base_sleep=0.8):
    """
    GET con reintentos y backoff exponencial + jitter.
    Devuelve dict JSON o lanza RuntimeError con el último error.
    """
    last = None
    for i in range(1, tries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200 and r.text.strip().startswith("{"):
                return r.json()
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = str(e)
        # backoff + jitter
        time.sleep(base_sleep * (2 ** (i - 1)) + random.uniform(0, 0.4))
    raise RuntimeError(f"Fallo GET JSON: {url}?{params} ({last})")

def normalize_draw(game_key, raw):
    """Convierte el objeto crudo LAE a nuestro formato común."""
    # LAE usa campos como 'fechaSorteo','combinacion','reintegro','complementario','clave','estrellas'
    fecha = (raw.get("fecha_sorteo") or raw.get("fechaSorteo") or raw.get("fecha") or "").strip()
    if not fecha:
        return None

    # Normalización de números
    numeros = []
    comb = (
        raw.get("combinacion")
        or raw.get("combinacionNumeros")
        or raw.get("numeros")
        or raw.get("bolas")
        or ""
    )
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

    # Complementarios / reintegro / estrellas / clave
    complementario = raw.get("complementario")
    reintegro = raw.get("reintegro")
    clave = raw.get("clave")  # el GORDO a veces usa 'clave'
    e1 = raw.get("estrella1") or raw.get("estrella_1")
    e2 = raw.get("estrella2") or raw.get("estrella_2")

    out = {
        "game": game_key,
        "date": fecha,
        "numbers": numeros[:6] if numeros else [],
    }
    if complementario is not None:
        out["complementario"] = complementario
    if reintegro is not None:
        out["reintegro"] = reintegro
    if clave is not None:
        out["clave"] = clave
    # Euromillones estrellas
    estrellas = []
    if e1 is not None:
        estrellas.append(e1)
    if e2 is not None:
        estrellas.append(e2)
    if estrellas:
        out["estrellas"] = estrellas
    return out

def fetch_year_for_variants(game_key, year, variants):
    start = f"{year}-01-01"
    end   = f"{year}-12-31"
    all_draws = []
    # probamos varias variantes de game_id
    for gid in variants:
        params = {
            "game_id": gid,
            "fechaInicioInclusiva": start,
            "fechaFinInclusiva": end,
        }
        try:
            data = retry_get(BASE, params)
            # LAE devuelve estructura con 'busqueda' o 'sorteos'
            # intentemos encontrar lista de sorteos
            items = (
                data.get("busqueda")
                or data.get("sorteos")
                or data.get("resultados")
                or data.get("buscador")
                or []
            )
            if isinstance(items, dict):
                # buscar una clave que sea lista
                for v in items.values():
                    if isinstance(v, list):
                        items = v
                        break
            if not isinstance(items, list):
                items = []

            parsed = []
            for raw in items:
                d = normalize_draw(game_key, raw)
                if d and d["date"]:
                    parsed.append(d)

            if parsed:
                print(f"[OK] {game_key} {year} via '{gid}' -> {len(parsed)} sorteos")
                all_draws.extend(parsed)
                # éxito: no probamos más variantes este año
                break
            else:
                print(f"[warn] {game_key} {year} '{gid}' respondió sin sorteos")
        except Exception as e:
            print(f"[fail] {game_key} {year} ({gid}): {e}")
        # espera breve antes de próxima variante
        time.sleep(0.6 + random.uniform(0, 0.4))
    return all_draws

def fetch_full_history():
    out = {k: [] for k in GAMES.keys()}
    for game_key, variants in GAMES.items():
        for year in range(START_YEAR, END_YEAR + 1):
            draws = fetch_year_for_variants(game_key, year, variants)
            out[game_key].extend(draws)
            # pausa corta entre años
            time.sleep(0.4 + random.uniform(0, 0.4))
    return out

def latest_by_game(draws):
    def parse_d(d):
        # LAE suele usar dd/MM/yyyy o yyyy-MM-dd
        s = d["date"]
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
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
            dt = parse_d(d) or best_dt
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
    print(f"[cfg] Años: {START_YEAR}..{END_YEAR}")
    ensure_dir(OUT_DIR)
    all_draws = fetch_full_history()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "results": sum(all_draws.values(), []),  # lista plana
        "by_game_counts": {k: len(v) for k, v in all_draws.items()},
    }

    # guarda maestro
    with open(os.path.join(OUT_DIR, "lae_historico.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # particionados
    for g, arr in all_draws.items():
        with open(os.path.join(OUT_DIR, f"{g}.json"), "w", encoding="utf-8") as f:
            json.dump(
                {"generated_at": payload["generated_at"], "results": arr},
                f,
                ensure_ascii=False,
                indent=2,
            )

    # latest
    latest = latest_by_game(all_draws)
    with open(os.path.join(OUT_DIR, "lae_latest.json"), "w", encoding="utf-8") as f:
        json.dump(
            {"generated_at": payload["generated_at"], "results": list(latest.values())},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print("=== LAE · HISTÓRICO · done ===")
    print("by_game_counts:", payload["by_game_counts"])

if __name__ == "__main__":
    main()
