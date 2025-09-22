#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, hashlib, json, time
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE = Path(__file__).resolve().parents[2]   # repo root
DATA_IN = BASE / "loterias" / "data"         # entrada: generado por sheets_to_csv.py
OUT_DIR = BASE / "dist" / "loterias" / "normalized"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = BASE / "dist" / f"loterias_manifest_{datetime.now().strftime('%Y%m%d')}.csv"

# Columnas “amables”
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
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

# Intenta detectar una columna de fecha común
CANDIDATE_DATE_COLS = ["fecha", "date", "dia", "day", "fechajuego", "fechasorteo"]

def add_common_fields(df: pd.DataFrame, src_name: str) -> pd.DataFrame:
    df = df.copy()
    df["source_file"] = src_name
    # Si el nombre insinúa el juego
    juego = None
    for key in ["primitiva", "bonoloto", "gordo", "euromillones", "euro", "euro_millones"]:
        if key in src_name.lower():
            juego = key.replace("_", "")
            break
    if juego:
        df["juego"] = juego

    # Normaliza una columna fecha_estandar si encuentra alguna
    for col in CANDIDATE_DATE_COLS:
        if col in df.columns:
            # No explotes si no es parseable
            try:
                df["fecha_estandar"] = pd.to_datetime(df[col], errors="coerce").dt.date
            except Exception:
                df["fecha_estandar"] = pd.NaT
            break
    return df

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_one(csv_path: Path) -> tuple[int, Path]:
    """Lee CSV, normaliza columnas, añade campos estándar, guarda en OUT_DIR con el mismo nombre."""
    try:
        df = pd.read_csv(csv_path)
    except UnicodeDecodeError:
        # fallback a latin-1 si viene raro
        df = pd.read_csv(csv_path, encoding="latin-1")
    except Exception as e:
        print(f"❌ Error leyendo {csv_path.name}: {e}")
        return 0, None

    df = normalize_columns(df)
    df = add_common_fields(df, csv_path.name)

    out_path = OUT_DIR / csv_path.name
    try:
        df.to_csv(out_path, index=False)
        print(f"✓ Normalizado: {csv_path.name} → {out_path.relative_to(BASE)} ({len(df)} filas)")
        return len(df), out_path
    except Exception as e:
        print(f"❌ Error guardando {out_path.name}: {e}")
        return 0, None

def build_manifest(files_out: list[Path]) -> None:
    rows = []
    for p in files_out:
        if p is None or not p.exists():
            continue
        rows.append({
            "file": str(p.relative_to(BASE)),
            "rows": sum(1 for _ in p.open("r", encoding="utf-8", errors="ignore")) - 1 if p.stat().st_size > 0 else 0,
            "bytes": p.stat().st_size,
            "sha256": sha256_file(p),
            "mtime": datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds"),
        })
    # Guarda manifest
    with MANIFEST_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["file", "rows", "bytes", "sha256", "mtime"])
        w.writeheader()
        w.writerows(rows)
    print(f"✓ Manifest: {MANIFEST_PATH.relative_to(BASE)} ({len(rows)} ficheros)")

def main():
    print("Normalización Loterías · inicio")
    if not DATA_IN.exists():
        print(f"⚠️ No existe {DATA_IN} — ejecuta primero sheets_to_csv.py")
        sys.exit(0)

    files = sorted(DATA_IN.glob("*.csv"))
    if not files:
        print("⚠️ No hay CSVs de entrada en loterias/data/")
        sys.exit(0)

    outputs = []
    total_rows = 0
    for f in files:
        n, outp = normalize_one(f)
        total_rows += n
        outputs.append(outp)

    build_manifest([p for p in outputs if p is not None])

    print(f"Normalización Loterías · fin — archivos: {len(outputs)} · filas totales: {total_rows}")

if __name__ == "__main__":
    main()
