#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera docs/index.html con:
- Resumen de última ejecución (manifest, master CSV)
- Informe de Data Quality (dist/dq_report.txt)
- Enlaces de Drive (dist/drive_links.txt)

Se ejecuta localmente o en CI después del pipeline.
"""

import os, glob, csv, datetime as dt
from pathlib import Path

BASE = Path(__file__).resolve().parent
DIST = BASE / "dist"
DOCS = BASE / "docs"
DOCS.mkdir(exist_ok=True)

def find_latest_manifest():
    files = sorted(DIST.glob("loterias_manifest_*.csv"))
    return files[-1] if files else None

def read_manifest(path):
    rows = []
    try:
        with open(path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
    except Exception:
        pass
    return rows

def read_master():
    path = DIST / "loterias_master.csv"
    if not path.exists():
        return None, 0
    nrows = 0
    header = []
    try:
        with open(path, newline='', encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            for _ in reader:
                nrows += 1
    except Exception:
        pass
    return header, nrows

def read_text(path):
    if not path or not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""

def parse_drive_links(text):
    links = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Formatos soportados:
        # - "<archivo>\t<url>"
        # - "↑ Subido: <archivo> → <url>"
        # - "→ OK: <archivo> → <url>"
        for sep in ["\t", "→"]:
            if sep in line:
                parts = [p.strip(" -") for p in line.split(sep) if p.strip()]
                # intenta coger último elemento como URL y primero como nombre
                if len(parts) >= 2:
                    name = parts[0].replace("↑ Subido:", "").replace("OK:", "").strip()
                    url = parts[-1]
                    if url.startswith("http"):
                        links.append((name, url))
                break
    # quitar duplicados manteniendo orden
    seen = set()
    out = []
    for name,url in links:
        key = (name,url)
        if key in seen: 
            continue
        seen.add(key)
        out.append((name,url))
    return out[-10:]  # últimos 10

def html_escape(s):
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def build_html(manifest_rows, master_header, master_nrows, dq_report, drive_links):
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    manifest_name = manifest_rows[0].get("manifest", "") if manifest_rows else ""

    # Tabla manifest (si trae campos filename, rows, out_file…)
    manifest_table = ""
    if manifest_rows:
        cols = list(manifest_rows[0].keys())
        manifest_table += "<table><thead><tr>" + "".join(f"<th>{html_escape(c)}</th>" for c in cols) + "</tr></thead><tbody>"
        for r in manifest_rows:
            manifest_table += "<tr>" + "".join(f"<td>{html_escape(r.get(c,''))}</td>" for c in cols) + "</tr>"
        manifest_table += "</tbody></table>"
    else:
        manifest_table = "<p>(Sin manifest)</p>"

    # Master resumen
    master_block = "<p>(No existe loterias_master.csv)</p>"
    if master_header is not None:
        master_block = (
            f"<p><b>Columnas:</b> {len(master_header)}<br>"
            f"<b>Filas:</b> {master_nrows}</p>"
        )

    # DQ
    dq_block = "<pre>(Sin dq_report.txt)</pre>"
    if dq_report.strip():
        dq_block = f"<pre>{html_escape(dq_report)}</pre>"

    # Links
    links_block = "<p>(Sin drive_links.txt)</p>"
    if drive_links:
        links_block = "<ul>" + "".join(
            f'<li><a href="{html_escape(url)}" target="_blank" rel="noopener">{html_escape(name)}</a></li>'
            for name, url in drive_links
        ) + "</ul>"

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Panel Loterías · Fran Ops</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;}}
  h1,h2{{margin:0 0 12px}}
  .card{{border:1px solid #ddd;border-radius:8px;padding:16px;margin:16px 0;background:#fff}}
  table{{border-collapse:collapse;width:100%;font-size:14px}}
  th,td{{border:1px solid #e5e5e5;padding:6px 8px;text-align:left;vertical-align:top}}
  th{{background:#fafafa}}
  .muted{{color:#777}}
  .grid{{display:grid;grid-template-columns:1fr;gap:16px}}
  @media(min-width:980px){{.grid{{grid-template-columns:1fr 1fr;}}}}
  code,pre{{background:#f7f7f9;border:1px solid #eee;border-radius:6px;padding:8px;display:block;overflow:auto;}}
</style>
</head>
<body>
  <h1>Panel Loterías · Fran Ops</h1>
  <p class="muted">Generado: {now}</p>

  <div class="grid">
    <div class="card">
      <h2>Última ejecución</h2>
      <p class="muted">Manifest: {html_escape(manifest_name) or "(no detectado)"}</p>
      {manifest_table}
    </div>

    <div class="card">
      <h2>Master CSV</h2>
      {master_block}
    </div>

    <div class="card">
      <h2>Enlaces en Drive (recientes)</h2>
      {links_block}
    </div>

    <div class="card">
      <h2>Data Quality</h2>
      {dq_block}
    </div>
  </div>

  <p class="muted">Fuente: archivos en <code>dist/</code> del repositorio.</p>
</body>
</html>
"""

def main():
    DOCS.mkdir(exist_ok=True)

    manifest_path = find_latest_manifest()
    manifest_rows = read_manifest(manifest_path) if manifest_path else []
    master_header, master_nrows = read_master()
    dq_report = read_text(DIST / "dq_report.txt")
    drive_links = parse_drive_links(read_text(DIST / "drive_links.txt"))

    html = build_html(manifest_rows, master_header, master_nrows, dq_report, drive_links)
    out = DOCS / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"✓ Generado {out}")

if __name__ == "__main__":
    main()
