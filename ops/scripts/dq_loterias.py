#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Quality · Loterías
- Recorre loterias/data/*.csv
- Imprime filas por fichero
- Reglas específicas (entradas.csv: duplicados + fechas)
- Genera dist/dq_report.txt para el email/resumen
Nunca rompe el pipeline: devuelve WARN/FAIL y exit code 0
"""

import os
import sys
import glob
import textwrap
from typing import Dict, List, Tuple

import pandas as pd

# --- Rutas --------------------------------------------------------------------
ROOT_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR  = os.environ.get("LOT_DATA_DIR", os.path.join(ROOT_DIR, "loterias", "data"))
DIST_DIR  = os.environ.get("DIST_DIR",      os.path.join(ROOT_DIR, "dist"))
os.makedirs(DIST_DIR, exist_ok=True)

# --- Fechas de control (usar Timestamp para evitar comparaciones inválidas) ---
MIN_DATE = pd.Timestamp("2000-01-01")
MAX_DATE = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)  # mañana 00:00

# --- Utilidades de lectura robusta -------------------------------------------
def read_csv_robust(path: str) -> pd.DataFrame:
    """
    Lee un CSV “a prueba de bombas”:
    - dtype=str para no forzar tipos
    - on_bad_lines='skip' para saltar filas rotas
    - engine='python' para tolerar separadores/quotes raros
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(
            path,
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip",
            engine="python",
        )
    except Exception:
        # Último intento con latin-1
        try:
            return pd.read_csv(
                path,
                dtype=str,
                encoding="latin-1",
                on_bad_lines="skip",
                engine="python",
            )
        except Exception:
            return pd.DataFrame()

def list_csvs() -> List[str]:
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    return files

# --- Reglas de calidad --------------------------------------------------------
def check_entradas(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """Reglas específicas de entradas.csv"""
    warns, fails = [], []

    if df.empty:
        return warns, fails

    # Normalizar espacios
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Duplicados: considerar duplicada la fila completa
    dup_mask = df.duplicated(keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count > 0:
        warns.append(f"entradas.csv: {dup_count} filas duplicadas.")

    # Fechas fuera de rango: buscar columnas con 'fecha'
    date_cols = [c for c in df.columns if "fecha" in c.lower()]
    # Si existe una preferente
    prefer = [c for c in date_cols if "ultimo" in c.lower() or "próximo" in c.lower() or "proximo" in c.lower()]
    cols_to_check = prefer[:1] or date_cols[:1]  # toma 1 si existe, si no la primera con 'fecha'

    for col in cols_to_check:
        parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        # Comparar SOLO con Timestamp para evitar TypeError
        bad_mask = parsed.isna() | (~parsed.between(MIN_DATE, MAX_DATE))
        bad_count = int(bad_mask.sum())
        if bad_count > 0:
            warns.append(
                f"entradas.csv: fechas fuera de rango = {bad_count} (cols ['{col}'])."
            )

    return warns, fails

# (ganchos para reglas futuras por tipo de CSV)
CHECKS: Dict[str, callable] = {
    "entradas.csv": check_entradas,
}

# --- Render del informe -------------------------------------------------------
def format_header() -> str:
    return "Data Quality · Loterías · inicio"

def format_footer(total_before: int, total_after: int, warn: int, fail: int) -> str:
    status = "OK"
    if fail > 0:
        status = "FAIL"
    elif warn > 0:
        status = "WARN"
    return f"\nTotal filas antes: {total_before}\nTotal filas después: {total_after}\nData Quality → {status} (warn={warn}, fail={fail})"

def write_report(text: str) -> str:
    path = os.path.join(DIST_DIR, "dq_report.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")
    return path

# --- Main ---------------------------------------------------------------------
def main() -> None:
    lines: List[str] = []
    lines.append(format_header())

    csv_paths = list_csvs()
    lines.append(f"CSV detectados: {len(csv_paths)}")

    total_before = 0
    total_after  = 0
    warn_count   = 0
    fail_count   = 0

    for path in csv_paths:
        name = os.path.basename(path)
        df = read_csv_robust(path)

        # Totales
        rows = int(df.shape[0])
        total_before += rows

        # Mostrar conteo
        lines.append(f"✓ {name}: {rows} filas.")

        # Reglas específicas por nombre
        checker = None
        for key, fn in CHECKS.items():
            if name.lower() == key:
                checker = fn
                break

        if checker:
            warns, fails = checker(df)
            for w in warns:
                lines.append(f"⚠️  {w}")
            for e in fails:
                lines.append(f"❌  {e}")
            warn_count += len(warns)
            fail_count += len(fails)

        # (hook para limpiezas automáticas si hiciera falta)
        total_after += int(df.shape[0])

    # Si no hay CSV, deja rastro claro (pero sin romper)
    if not csv_paths:
        lines.append("— Sin CSV en loterias/data —")

    # Cierre
    lines.append(format_footer(total_before, total_after, warn_count, fail_count))
    report_text = "\n".join(lines)

    # Salida consola
    print(report_text)

    # Guardar para el email
    path = write_report(report_text)
    print(path)

    # No reventar el pipeline (exit 0 siempre)
    # Si algún día quieres fallar en FAIL, cambia a:
    #   sys.exit(1 if fail_count > 0 else 0)
    sys.exit(0)

if __name__ == "__main__":
    main()
