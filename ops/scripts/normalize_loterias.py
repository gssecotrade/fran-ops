#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normalización Loterías
Lee CSVs desde loterias/data/, estandariza columnas/fechas/nulos,
y guarda los resultados en dist/loterias/normalized/YYYY-MM-DD/.
Genera además un manifest con metadatos (filas, hash, bytes).

Diseñado para ser tolerante a formatos: coerciona errores y registra avisos.
"""

import os
import sys
import csv
import json
import hashlib
from datetime import datetime, date
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[2]
DATA_DIR = BASE / "loterias" / "data"
OUT_ROOT = BASE / "dist" / "loterias" / "normalized"

# Fecha de “corte” para la carpeta de salida (hoy)
TODAY = date.today().strftime("%Y-%m-%d")
OUT_DIR = OUT_ROOT / TODAY
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Columnas que intentaremos interpretar como fecha
DATE_CANDIDATES = {
    "fecha", "date", "fecha_sorteo", "fecha_bono", "fecha_gordo",
    "fecha_primitiva", "fecha_euromillones", "f_sorteo"
}

def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normalizar nombre de columnas
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Strip a strings
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            df[c] = df[c].astype("string").str.strip()

    # Intento de estandarizar una columna de fecha si existe
    date_cols = [c for c in df.columns if c in DATE_CANDIDATES]
    if date_cols:
        col = date_cols[0]
        # intentamos con dayfirst True (formato dd/mm/yyyy habitual)
        # y coercion de errores
        try:
            df["fecha_estandar"] = pd.to_datetime(
                df[col], errors="coerce", dayfirst=True
            ).dt.date
        except Exception:
            # fallback sin dayfirst por si acaso
            df["fecha_estandar"] = pd.to_datetime(
                df[col], errors="coerce"
            ).dt.date

    # Relleno de NaNs coherente
    df = df.fillna(pd.NA)

    return df

def normalize_file(in_path: Path, out_path: Path) -> dict:
    """Normaliza un CSV y lo escribe; devuelve metadatos para manifest."""
    # Lectura tolerante
    try:
        # Primer intento: lectura “rápida”
        df = pd.read_csv(in_path)
    except Exception:
        # Segundo intento: motor python, sep autodetect
        try:
            with in_path.open("r", newline="", encoding="utf-8") as fh:
                sniffer = csv.Sniffer()
                sample = fh.read(4096)
                fh.seek(0)
                delim = sniffer.sniff(sample).delimiter if sample else ","
            df = pd.read_csv(in_path, engine="python", sep=delim, on_bad_lines="skip")
        except Exception as e:
            print(f"⚠️  No pude parsear {in_path.name}: {e}", file=sys.stderr)
            # Creamos CSV vacío con cabecera “error”
            out_path.write_text("error\nparser_failed\n", encoding="utf-8")
            return {
                "file": in_path.name,
                "rows": 0,
                "bytes": out_path.stat().st_size,
                "sha256": sha256_of_file(out_path)
            }

    # Normalización
    df_norm = normalize_df(df)

    # Escritura
    df_norm.to_csv(out_path, index=False)

    return {
        "file": in_path.name,
        "rows": int(df_norm.shape[0]),
        "bytes": out_path.stat().st_size,
        "sha256": sha256_of_file(out_path),
    }

def main():
    print("Normalización Loterías · inicio")
    if not DATA_DIR.exists():
        print(f"⚠️  {DATA_DIR} no existe; nada que normalizar")
        return

    manifest = []
    for csv_path in sorted(DATA_DIR.glob("*.csv")):
        out_path = OUT_DIR / csv_path.name
        meta = normalize_file(csv_path, out_path)
        manifest.append(meta)
        print(f"✓ Normalizado: {csv_path.name} ({meta['rows']} filas)")

    # Guardamos manifest CSV
    manifest_csv = BASE / "dist" / f"loterias_manifest_{datetime.now().strftime('%Y%m%d')}.csv"
    pd.DataFrame(manifest).to_csv(manifest_csv, index=False)

    # Y manifest JSON (útil para máquinas)
    manifest_json = OUT_DIR / "manifest.json"
    manifest_json.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "out_dir": str(OUT_DIR),
        "files": manifest
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    total_rows = sum(m["rows"] for m in manifest)
    print(f"✓ Manifest: {manifest_csv} ({len(manifest)} ficheros)")
    print(f"Normalización Loterías · fin — archivos: {len(manifest)} · filas totales: {total_rows}")

if __name__ == "__main__":
    main()
