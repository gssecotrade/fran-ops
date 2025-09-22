"""
normalize_loterias.py
Normaliza los CSV generados desde Google Sheets para Loterías
y crea un manifest con hashes y número de filas.
"""

import os
import hashlib
import pandas as pd
from pathlib import Path

DATA_DIR = Path("loterias/data")
DIST_DIR = Path("dist/loterias/normalized")
DIST_DIR.mkdir(parents=True, exist_ok=True)

def sha256sum(file_path: Path) -> str:
    """Devuelve el hash SHA256 de un fichero."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_csv(file_path: Path, out_dir: Path) -> dict:
    """Lee, normaliza y guarda CSV. Devuelve metadatos para manifest."""
    df = pd.read_csv(file_path)

    # Normalización básica
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    # Guardar normalizado
    out_path = out_dir / file_path.name
    df.to_csv(out_path, index=False)

    return {
        "file": file_path.name,
        "rows": len(df),
        "sha256": sha256sum(out_path),
    }

def main():
    print("Normalización Loterías · inicio")

    manifest = []
    for csv_file in DATA_DIR.glob("*.csv"):
        meta = normalize_csv(csv_file, DIST_DIR)
        manifest.append(meta)
        print(f"✓ Normalizado: {csv_file.name} ({meta['rows']} filas)")

    # Crear manifest
    manifest_df = pd.DataFrame(manifest)
    manifest_file = Path("dist") / f"loterias_manifest.csv"
    manifest_df.to_csv(manifest_file, index=False)
    print(f"✓ Manifest: {manifest_file} ({len(manifest_df)} ficheros)")

if __name__ == "__main__":
    main()
