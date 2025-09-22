#!/usr/bin/env python3
"""
DQ Loterías: limpieza básica y checks.
- Lee CSVs en loterias/data
- Hace backup de los originales en loterias/data/.bak_timestamp/
- Para entradas.csv: elimina duplicados (mantiene primer registro)
- Para cualquier col que parezca fecha: comprueba parseo (dayfirst=True), elimina filas con fechas inválidas o fuera de rango razonable
- Genera dist/dq_report.txt con resumen (WARN/FAIL)
Exit code: 0 siempre (pipeline recoge WARNs) — cambiamos si quieres que FAIL detenga pipeline.
"""
from pathlib import Path
import pandas as pd
from datetime import date
import shutil
import sys

ROOT = Path.cwd()
DATA_DIR = ROOT / "loterias" / "data"
DIST_DIR = ROOT / "dist"
BACKUP_DIR = DATA_DIR / f".bak_{pd.Timestamp.today().strftime('%Y%m%d_%H%M%S')}"
REPORT_PATH = DIST_DIR / "dq_report.txt"

MIN_DATE = pd.Timestamp("2000-01-01").date()
MAX_DATE = (pd.Timestamp.today() + pd.Timedelta(days=1)).date()

def safe_mkdir(p: Path):
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

def read_csv_robust(path: Path):
    # use on_bad_lines='skip' for robustness
    try:
        return pd.read_csv(path, on_bad_lines="skip", dtype=str)
    except Exception as e:
        # fallback using engine python
        return pd.read_csv(path, engine="python", on_bad_lines="skip", dtype=str)

def find_date_cols(df: pd.DataFrame):
    # heurístico: nombres que contienen 'fecha' o 'date'
    return [c for c in df.columns if "fecha" in c.lower() or "date" in c.lower()]

def parse_date_col(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date

def main():
    safe_mkdir(DIST_DIR)
    report_lines = []
    warn_count = 0
    fail_count = 0

    if not DATA_DIR.exists():
        report_lines.append(f"ERROR: carpeta {DATA_DIR} no existe.")
        fail_count += 1
        with open(REPORT_PATH, "w") as f:
            f.write("\n".join(report_lines))
        print("\n".join(report_lines))
        sys.exit(1)

    safe_mkdir(BACKUP_DIR)
    # backup originals
    for f in DATA_DIR.glob("*.csv"):
        shutil.copy2(f, BACKUP_DIR / f.name)

    csvs = sorted(DATA_DIR.glob("*.csv"))
    report_lines.append(f"Data Quality · Loterías · inicio")
    report_lines.append(f"CSV detectados: {len(csvs)}")

    total_rows_before = 0
    total_rows_after = 0

    for csv_path in csvs:
        name = csv_path.name
        df = read_csv_robust(csv_path)
        n_before = len(df)
        total_rows_before += n_before

        per_file_notes = []
        # Trim whitespace on string columns
        for c in df.select_dtypes(include=["object"]).columns:
            df[c] = df[c].astype(str).str.strip()

        # Special: entradas.csv -> dedupe
        if name.lower() == "entradas.csv":
            # consider all columns to detect exact duplicates
            n_dup = df.duplicated(keep="first").sum()
            if n_dup > 0:
                df = df.drop_duplicates(keep="first")
                per_file_notes.append(f"{n_dup} filas duplicadas eliminadas")
                warn_count += 1

        # Check date columns
        date_cols = find_date_cols(df)
        date_issues = 0
        if date_cols:
            for col in date_cols:
                parsed = parse_date_col(df[col])
                bad_mask = parsed.isna() | (~parsed.between(MIN_DATE, MAX_DATE))
                date_issues += int(bad_mask.sum())
                if bad_mask.any():
                    # remove rows with invalid date in that column
                    df = df[~bad_mask]
                    per_file_notes.append(f"{bad_mask.sum()} filas eliminadas por fechas inválidas en {col}")
                    warn_count += 1

        n_after = len(df)
        total_rows_after += n_after

        # Overwrite CSV with cleaned version
        df.to_csv(csv_path, index=False)

        if per_file_notes:
            report_lines.append(f"⚠️ {name}: {n_after} filas · notes: {', '.join(per_file_notes)}")
        else:
            report_lines.append(f"✓ {name}: {n_after} filas.")

    report_lines.append("")
    report_lines.append(f"Total filas antes: {total_rows_before}")
    report_lines.append(f"Total filas después: {total_rows_after}")
    report_lines.append(f"Data Quality → WARN (warn={warn_count}, fail={fail_count})")

    # write report
    safe_mkdir(DIST_DIR)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    print("\n".join(report_lines))
    # exit 0 so pipeline continues; if fail_count>0 you can change to non-zero to stop pipeline
    sys.exit(0)

if __name__ == "__main__":
    main()
