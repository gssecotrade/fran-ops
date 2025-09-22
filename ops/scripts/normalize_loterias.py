#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import hashlib
from datetime import datetime
import pandas as pd

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE, "loterias", "data")
NORM_DIR = os.path.join(BASE, "loterias", "normalized")
DIST_DIR = os.path.join(BASE, "dist")

# Columnas “fecha” más probables en tus ficheros
DATE_CANDIDATES = [
    "fecha", "Fecha", "date", "fecha_sorteo", "fecha_compra",
    "fecha_bono", "fecha_gordo", "fecha_euro", "created_at"
]

# Tipado “suave” para mantener consistencia entre runs
SCHEMA_HINT = {
    "juego": "string",
    "tipo": "string",
    "bote": "float64",
    "importe": "float64",
    "premio": "float64",
    "apuestas": "Int64",
    "aciertos": "Int64",
    "user_id": "string",
    "email": "string",
    "numero": "string",
    "serie": "string",
    "fraccion": "string",
}

def ensure_dirs():
    os.makedirs(NORM_DIR, exist_ok=True)
    os.makedirs(DIST_DIR, exist_ok=True)

def read_csv_safely(path: str) -> pd.DataFrame:
    # Maneja separador y encoding típicos sin reventar
    for sep in [",", ";", "\t", "|"]:
        try:
            df = pd.read_csv(path, sep=sep, engine="python")
            if df.shape[1] > 1:
                return df
        except Exception:
            pass
    # Último intento “normal”
    return pd.read_csv(path)

def coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza fechas sin warnings y con dayfirst=True (formato español)
    for col in DATE_CANDIDATES:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            # Añadimos una columna estandar si existe alguna fecha util
            if "fecha_estandar" not in df.columns:
                df["fecha_estandar"] = pd.NaT
            df.loc[df[col].notna(), "fecha_estandar"] = df[col]
    if "fecha_estandar" in df.columns:
        # como date “limpio”
        df["fecha_estandar"] = df["fecha_estandar"].dt.date
    return df

def apply_schema_hint(df: pd.DataFrame) -> pd.DataFrame:
    for col, dtype in SCHEMA_HINT.items():
        if col in df.columns:
            try:
                if dtype == "string":
                    df[col] = df[col].astype("string")
                else:
                    df[col] = df[col].astype(dtype)
            except Exception:
                # Si castea mal, lo dejamos tal cual para no romper
                pass
    return df

def shortname(fname: str) -> str:
    return os.path.splitext(os.path.basename(fname))[0]

def normalize_one(file_path: str) -> dict:
    df = read_csv_safely(file_path)
    df = coerce_dates(df)
    df = apply_schema_hint(df)
    # Orden estable de columnas: fecha_estandar primero si existe
    cols = list(df.columns)
    if "fecha_estandar" in cols:
        cols = ["fecha_estandar"] + [c for c in cols if c != "fecha_estandar"]
        df = df[cols]

    out_csv = os.path.join(NORM_DIR, os.path.basename(file_path))
    df.to_csv(out_csv, index=False)

    meta = {
        "file": os.path.basename(file_path),
        "rows": len(df),
        "cols": len(df.columns),
        "out": out_csv,
    }
    print(f"✓ Normalizado: {os.path.basename(file_path)} ({meta['rows']} filas)")
    return meta

def write_manifest(metas: list) -> str:
    stamp = datetime.now().strftime("%Y%m%d")
    manifest_path = os.path.join(DIST_DIR, f"loterias_manifest_{stamp}.csv")
    mdf = pd.DataFrame(metas).sort_values("file")
    mdf.to_csv(manifest_path, index=False)
    return manifest_path

def write_master(metas: list) -> tuple[str, str]:
    # Concatena todo en un master.csv + master.parquet
    frames = []
    for m in metas:
        try:
            frames.append(pd.read_csv(m["out"]))
        except Exception:
            pass
    if not frames:
        return "", ""
    master = pd.concat(frames, ignore_index=True)
    master_csv = os.path.join(DIST_DIR, "loterias_master.csv")
    master_parquet = os.path.join(DIST_DIR, "loterias_master.parquet")
    master.to_csv(master_csv, index=False)
    try:
        master.to_parquet(master_parquet, index=False)
    except Exception:
        master_parquet = ""  # parquet opcional
    return master_csv, master_parquet

def main():
    ensure_dirs()
    print("Normalización Loterías · inicio")
    csvs = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    metas = []
    for fp in csvs:
        try:
            metas.append(normalize_one(fp))
        except Exception as e:
            print(f"⚠️  Error normalizando {os.path.basename(fp)}: {e}")

    manifest = write_manifest(metas) if metas else ""
    master_csv, master_parquet = write_master(metas) if metas else ("", "")

    total_rows = sum(m.get("rows", 0) for m in metas)
    print(f"✓ Manifest: {manifest if manifest else '—'}")
    if master_csv:
        print(f"✓ Master CSV: {master_csv}")
    if master_parquet:
        print(f"✓ Master Parquet: {master_parquet}")

    print(f"Normalización Loterías · fin — archivos: {len(metas)} · filas totales: {total_rows}")

if __name__ == "__main__":
    main()
