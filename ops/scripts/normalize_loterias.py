#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normalización robusta para Loterías:
- Lee todos los CSV en loterias/data
- Tolera filas corruptas (on_bad_lines='skip') y separadores variados
- Normaliza nombres de columnas, añade campos auxiliares (source_file, juego, fecha_estandar)
- Guarda CSVs normalizados en dist/loterias/normalized
- Genera manifest con filas, bytes, sha256 y mtime
"""

import csv
import hashlib
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE = Path(__file__).resolve().parents[2]
DATA_IN = BASE / "loterias" / "data"
OUT_DIR = BASE / "dist" / "loterias" / "normalized"
OUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = BASE / "dist" / f"loterias_manifest_{datetime.now().strftime('%Y%m%d')}.csv"

CANDIDATE_DATE_COLS = ["fecha", "date", "dia", "day", "fechajuego", "fechasorteo"]

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

def sniff_delimiter(path: Path) -> str | None:
    """Intenta detectar delimitador con csv.Sniffer; si falla, None (pandas inferirá)."""
    try:
        sample = path.read_text(encoding="utf-8", errors="ignore")[:10000]
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return None

def read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Lectura tolerante:
    1) utf-8, delimitador inferido, engine=python, on_bad_lines='skip'
    2) si falla, latin-1 con mismas opciones
    3) si sigue fallando, fuerza separador detectado (sniff) y vuelve a intentar
    """
    delim = sniff_delimiter(path)
    common_kwargs = dict(engine="python", on_bad_lines="skip")
    # 1) utf-8
    try:
        return pd.read_csv(path, sep=delim if delim else None, encoding="utf-8", **common_kwargs)
    except Exception:
        pass
    # 2) latin-1
    try:
        return pd.read_csv(path, sep=delim if delim else None, encoding="latin-1", **common_kwargs)
    except Exception:
        pass
    # 3) fuerzo separadores candidatos si no hubo sniff
    for sep in [",", ";", "\t", "|"]:
        try:
            return pd.read_csv(path, sep=sep, encoding="utf-8", **common_kwargs)
        except Exception:
            try:
                return pd.read_csv(path, sep=sep, encoding="latin-1", **common_kwargs)
            except Exception:
                continue
    raise RuntimeError(f"No se pudo leer de forma robusta: {path.name}")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.lower()
        .str.replace("[áàä]", "a", regex=True)
        .str.replace("[éèë]", "e", regex=True)
        .str.replace("[íìï]", "i", regex=True)
        .str.replace("[óòö]", "o", regex=True)
        .str.replace("[úùü]", "u", regex=True)
        .str.replace("[^a-z0-9_]", "", regex=True)
    )
    return df

def add_common_fields(df: pd.DataFrame, src_name: str) -> pd.DataFrame:
    df = df.copy()
    df["source_file"] = src_name
    juego = None
    for key in ["primitiva", "bonoloto", "gordo", "euromillones", "euro", "euro_millones"]:
        if key in src_name.lower():
            juego = key.replace("_", "")
            break
    if juego:
        df["juego"] = juego
    for col in CANDIDATE_DATE_COLS:
        if col in df.columns:
            try:
                df["fecha_estandar"] = pd.to_datetime(df[col], errors="coerce").dt.date
            except Exception:
                df["fecha_estandar"] = pd.NaT
            break
    return df

def normalize_one(csv_path: Path) -> tuple[int, Path | None]:
    try:
        df = read_csv_robust(csv_path)
    except Exception as e:
        print(f"❌ Error leyendo {csv_path.name}: {e}")
        return 0, None

    # Limpiezas mínimas
    df = df.drop_duplicates().reset_index(drop=True)
    df = normalize_columns(df)
    df = add_common_fields(df, csv_path.name)

    out_path = OUT_DIR / csv_path.name
    try:
        df.to_csv(out_path, index=False)
        print(f"✓ Normalizado: {csv_path.name} ({len(df)} filas)")
        return len(df), out_path
    except Exception as e:
        print(f"❌ Error guardando {out_path.name}: {e}")
        return 0, None

def build_manifest(files_out: list[Path]) -> None:
    rows = []
    for p in files_out:
        if p is None or not p.exists():
            continue
        try:
            rows.append({
                "file": str(p.relative_to(BASE)),
                "rows": max(0, sum(1 for _ in p.open("r", encoding="utf-8", errors="ignore")) - 1),
                "bytes": p.stat().st_size,
                "sha256": sha256_file(p),
                "mtime": datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds"),
            })
        except Exception as e:
            print(f"⚠️ No se pudo indexar {p.name} en manifest: {e}")

    mf = pd.DataFrame(rows, columns=["file", "rows", "bytes", "sha256", "mtime"])
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    mf.to_csv(MANIFEST_PATH, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✓ Manifest: {MANIFEST_PATH.relative_to(BASE)} ({len(mf)} ficheros)")

def main():
    print("Normalización Loterías · inicio")
    if not DATA_IN.exists():
        print(f"⚠️ No existe {DATA_IN} — ejecuta primero sheets_to_csv.py")
        return

    files = sorted(DATA_IN.glob("*.csv"))
    if not files:
        print("⚠️ No hay CSVs de entrada en loterias/data/")
        return

    outputs, total_rows = [], 0
    for f in files:
        n, outp = normalize_one(f)
        total_rows += n
        outputs.append(outp)

    build_manifest([p for p in outputs if p])
    print(f"Normalización Loterías · fin — archivos: {len([p for p in outputs if p])} · filas totales: {total_rows}")

if __name__ == "__main__":
    main()
