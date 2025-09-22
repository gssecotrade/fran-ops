#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Quality para Loterías:
- Existencia y filas > 0
- Fechas plausibles (si hay columnas de fecha)
- Duplicados (por fila completa)
- Diferencias vs. último manifest (conteos)
Escribe resumen en dist/dq_report.txt y no rompe el pipeline (exit 0).
"""

import os, glob, sys, json
from datetime import date, timedelta
import pandas as pd

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE, "loterias", "data")
DIST_DIR = os.path.join(BASE, "dist")
os.makedirs(DIST_DIR, exist_ok=True)

REPORT_PATH = os.path.join(DIST_DIR, "dq_report.txt")

# Heurística de columnas de fecha
DATE_COL_HINTS = {"fecha", "date", "dia", "día", "fecharegistro", "fechapago"}

def safe_read_csv(path: str) -> pd.DataFrame:
    kw = dict(
        sep=",",
        on_bad_lines="skip",  # tolerante
        encoding="utf-8",
    )
    try:
        return pd.read_csv(path, **kw)
    except Exception:
        # Reintento flexible
        return pd.read_csv(path, **{**kw, "engine": "python"})

def find_date_cols(df: pd.DataFrame) -> list[str]:
    cols = []
    for c in df.columns:
        cname = str(c).strip().lower()
        if any(h in cname for h in DATE_COL_HINTS):
            cols.append(c)
    return cols

def check_dates(df: pd.DataFrame) -> dict:
    res = {"checked_cols": [], "out_of_range": 0, "rows": len(df)}
    today = date.today()
    min_ok = date(2000, 1, 1)
    for col in find_date_cols(df):
        try:
            parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True).dt.date
            oor = ((parsed < min_ok) | (parsed > (today + timedelta(days=1)))).sum()
            res["out_of_range"] += int(oor)
            res["checked_cols"].append(col)
        except Exception:
            # si no se puede parsear, lo ignoramos
            pass
    return res

def compare_with_prev_manifest() -> list[str]:
    msgs = []
    manifests = sorted(glob.glob(os.path.join(DIST_DIR, "loterias_manifest_*.csv")))
    if len(manifests) < 2:
        msgs.append("Δ Sin manifest previo suficiente para comparar.")
        return msgs

    prev, curr = manifests[-2], manifests[-1]
    mp = pd.read_csv(prev)
    mc = pd.read_csv(curr)

    # esperamos columnas: file, rows. Si no están, lo indicamos.
    if not {"file", "rows"}.issubset(set(mc.columns)) or not {"file", "rows"}.issubset(set(mp.columns)):
        msgs.append("Δ Manifest sin columnas esperadas (file, rows).")
        return msgs

    prev_map = dict(zip(mp["file"], mp["rows"]))
    curr_map = dict(zip(mc["file"], mc["rows"]))

    for f, rows in curr_map.items():
        before = prev_map.get(f)
        if before is None:
            msgs.append(f"Δ {f}: nuevo (antes no existía).")
        else:
            delta = int(rows) - int(before)
            if delta != 0:
                msgs.append(f"Δ {f}: {before} → {rows} ({'+' if delta>0 else ''}{delta})")

    missing = [f for f in prev_map.keys() if f not in curr_map]
    for f in missing:
        msgs.append(f"Δ {f}: desaparecido respecto a manifest previo.")

    if not msgs:
        msgs.append("Δ Sin cambios de conteo respecto al manifest previo.")
    return msgs

def main():
    lines = []
    status = "PASS"
    warn_cnt = 0
    fail_cnt = 0

    csvs = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not csvs:
        status = "FAIL"
        lines.append("No hay CSVs en loterias/data/.")
    else:
        lines.append(f"CSV detectados: {len(csvs)}")

    for path in csvs:
        name = os.path.basename(path)
        try:
            df = safe_read_csv(path)
        except Exception as e:
            status = "FAIL"
            fail_cnt += 1
            lines.append(f"❌ {name}: error leyendo CSV → {e}")
            continue

        rows = len(df)
        if rows == 0:
            warn_cnt += 1
            lines.append(f"⚠️  {name}: 0 filas.")
        else:
            lines.append(f"✓ {name}: {rows} filas.")

        # Duplicados por fila completa
        dups = int(df.duplicated().sum())
        if dups > 0:
            warn_cnt += 1
            lines.append(f"⚠️  {name}: {dups} filas duplicadas.")

        # Fechas plausibles
        dchk = check_dates(df)
        if dchk["checked_cols"]:
            if dchk["out_of_range"] > 0:
                warn_cnt += 1
                lines.append(f"⚠️  {name}: fechas fuera de rango = {dchk['out_of_range']} (cols {dchk['checked_cols']}).")

    # Comparación con manifest previo
    lines.append("")
    lines.extend(compare_with_prev_manifest())

    # Resumen
    if fail_cnt > 0:
        status = "FAIL"
    elif warn_cnt > 0:
        status = "WARN"
    else:
        status = "PASS"

    header = f"Data Quality · Loterías → {status} (warn={warn_cnt}, fail={fail_cnt})"
    report = header + "\n" + "\n".join(lines) + "\n"

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)
    # No rompemos el pipeline
    sys.exit(0)

if __name__ == "__main__":
    main()
