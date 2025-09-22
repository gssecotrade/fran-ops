#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, glob, hashlib
from datetime import datetime
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "loterias", "data")
DIST_DIR = os.path.join(BASE_DIR, "dist")
OUT_DIR  = os.path.join(DIST_DIR, "loterias_norm")

os.makedirs(DIST_DIR, exist_ok=True)
os.makedirs(OUT_DIR,  exist_ok=True)

# --- Utilidades --------------------------------------------------------------

def _try_read(path, **kw):
    """Intento básico de lectura con pandas."""
    return pd.read_csv(path, **kw)

def robust_read_csv(path: str) -> pd.DataFrame:
    """
    Lector robusto:
      1) Autodetección con engine='python', sep=None
      2) Reintentos con separadores comunes
      3) on_bad_lines='skip' para saltar líneas corruptas
    """
    common_kw = dict(encoding="utf-8", dtype=str)
    # 1) Autodetección
    try:
        return _try_read(path, sep=None, engine="python", on_bad_lines="warn", **common_kw)
    except Exception:
        pass

    # 2) Reintentos por separador
    for sep in [",", ";", "\t", "|"]:
        try:
            return _try_read(path, sep=sep, engine="python", on_bad_lines="warn", **common_kw)
        except Exception:
            continue

    # 3) Último recurso: sin encabezado, luego renombramos
    try:
        df = _try_read(path, header=None, engine="python", on_bad_lines="skip", **common_kw)
        # genera nombres de columnas genéricos
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
        return df
    except Exception as e:
        raise RuntimeError(f"No se pudo leer {path}: {e}")

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza general: espacios, columnas totalmente vacías y normalización básica."""
    # Elimina columnas vacías completas
    df = df.dropna(axis=1, how="all")
    # Strip a nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]
    # Strip a strings
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

def add_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Añade una columna _rowhash para trazabilidad."""
    def row_hash(s: pd.Series) -> str:
        return hashlib.md5("|".join(s.fillna("").astype(str).tolist()).encode("utf-8")).hexdigest()
    df["_rowhash"] = df.apply(row_hash, axis=1)
    return df

def save_csv(df: pd.DataFrame, out_path: str):
    df.to_csv(out_path, index=False, encoding="utf-8")

# --- Normalizadores específicos ---------------------------------------------

def normalize_salidas(path: str) -> pd.DataFrame:
    """
    'salidas.csv' a veces trae 2 columnas esperadas pero líneas con comas extra.
    Estrategia:
      - Leer robusto
      - Si hay >2 columnas, unificar desde la 2ª en un único campo 'detalle'
      - Renombrar a ['fecha', 'detalle'] si procede
    """
    df = robust_read_csv(path)
    df = clean_df(df)

    # Si llega con encabezados típicos intentamos detectarlos
    cols = list(df.columns)
    if len(cols) >= 2:
        # Si hay más de 2 columnas, las comprimimos
        if len(cols) > 2:
            first_col = cols[0]
            rest_cols = cols[1:]
            df["detalle"] = df[rest_cols].apply(
                lambda r: ", ".join([x for x in r.astype(str).tolist() if x and x.lower() != "nan"]),
                axis=1
            )
            df["fecha"] = df[first_col]
            df = df[["fecha", "detalle"]]
        else:
            # Mantén las dos
            df.columns = ["fecha", "detalle"]
    else:
        # Sin columnas claras: fuerza 2
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
        if df.shape[1] == 1:
            df["detalle"] = ""
        df.rename(columns={"col_1": "fecha", "col_2": "detalle"}, inplace=True)

    # Normaliza fecha si es posible (dayfirst=True por formato español)
    if "fecha" in df.columns:
        df["fecha_estandar"] = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True).dt.date

    return add_hash(df)

def normalize_generic(path: str) -> pd.DataFrame:
    df = robust_read_csv(path)
    df = clean_df(df)

    # Heurística: si hay columna fecha, normalízala a fecha_estandar
    for col in df.columns:
        if "fecha" in col.lower():
            try:
                df["fecha_estandar"] = pd.to_datetime(df[col], errors="coerce", dayfirst=True).dt.date
                break
            except Exception:
                continue

    return add_hash(df)

# --- Pipeline ----------------------------------------------------------------

def normalize_file(csv_path: str, out_dir: str) -> dict:
    name = os.path.basename(csv_path)
    try:
        if name.lower() == "salidas.csv":
            df = normalize_salidas(csv_path)
        else:
            df = normalize_generic(csv_path)

        out_path = os.path.join(out_dir, name)
        save_csv(df, out_path)
        print(f"✓ Normalizado: {name} ({len(df)} filas)")
        return {"file": name, "rows": len(df), "ok": True}
    except Exception as e:
        print(f"⚠️  Error normalizando {name}: {e}")
        return {"file": name, "rows": 0, "ok": False, "error": str(e)}

def build_manifest(results: list, out_dir: str) -> str:
    ts = datetime.now().strftime("%Y%m%d")
    mani = os.path.join(DIST_DIR, f"loterias_manifest_{ts}.csv")
    rows = []
    total = 0
    for r in results:
        if r.get("ok"):
            total += r["rows"]
        rows.append(r)
    md = pd.DataFrame(rows)
    md.to_csv(mani, index=False, encoding="utf-8")
    print(f"✓ Manifest: {mani}")
    print(f"Normalización Loterías · fin — archivos: {sum(1 for r in results if r.get('ok'))} · filas totales: {total}")
    return mani

def build_master_csv(out_dir: str) -> str:
    """
    Une todos los CSV normalizados en uno solo (añade columna _source).
    """
    master_path = os.path.join(DIST_DIR, "loterias_master.csv")
    parts = []
    for f in sorted(glob.glob(os.path.join(out_dir, "*.csv"))):
        try:
            df = pd.read_csv(f, dtype=str, encoding="utf-8")
            df["_source"] = os.path.basename(f)
            parts.append(df)
        except Exception:
            continue
    if parts:
        pd.concat(parts, ignore_index=True).to_csv(master_path, index=False, encoding="utf-8")
        print(f"✓ Master CSV: {master_path}")
    return master_path

def main():
    print("Normalización Loterías · inicio")
    results = []
    for csv_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.csv"))):
        meta = normalize_file(csv_file, OUT_DIR)
        results.append(meta)

    manifest_path = build_manifest(results, OUT_DIR)
    build_master_csv(OUT_DIR)
    print("Listo en", DIST_DIR)

if __name__ == "__main__":
    main()
