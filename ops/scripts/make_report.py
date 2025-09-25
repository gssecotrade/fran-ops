#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera:
  - dist/report.json  (datos estructurados del último run + series históricas)
  - docs/index.html   (panel DQ con gráficas y enlaces)

Entrada esperada en dist/:
  - loterias_manifest_YYYYMMDD.csv      (uno por run; usamos todos para series)
  - loterias_master.csv                 (opcional, sólo informativo)
  - dq_report.txt                       (resumen DQ del último run)
  - drive_links.txt                     (3 líneas: legales, loterias, marketing)
  - *.zip                               (mostramos los del último run)
"""

import os, re, csv, json, glob, html, pathlib
from datetime import datetime

DIST = os.environ.get("DIST_DIR", "dist")
DOCS_DIR = "docs"
REPORT_JSON = os.path.join(DIST, "report.json")
HTML_OUT = os.path.join(DOCS_DIR, "index.html")

# ------------------------ utilidades --------------------------------
def read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""

def find_manifests() -> list:
    pats = sorted(glob.glob(os.path.join(DIST, "loterias_manifest_*.csv")))
    # Orden por fecha incluida en el nombre
    def key(p):
        m = re.search(r"_(\d{8})", p)  # YYYYMMDD
        return m.group(1) if m else "00000000"
    return sorted(pats, key=key)

def parse_manifest_rows(manifest_path: str) -> dict:
    """
    Devuelve { 'date':'YYYY-MM-DD', 'csv_count':N, 'rows_total':M }
    Intenta sumar una columna 'rows' (o similar). Si no existe, usa 0.
    """
    date_str = "1970-01-01"
    m = re.search(r"_(\d{8})", manifest_path)
    if m:
        y,mn,d = m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:8]
        date_str = f"{y}-{mn}-{d}"

    rows_total = 0
    csv_count = 0
    header = []
    rows_idx = None

    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for i, r in enumerate(reader):
                if i == 0:
                    header = [c.strip().lower() for c in r]
                    # buscamos una columna de filas
                    for cand in ("rows","row_count","n_rows","filas","count"):
                        if cand in header:
                            rows_idx = header.index(cand)
                            break
                else:
                    if not r or all(x.strip()=="" for x in r):
                        continue
                    csv_count += 1
                    if rows_idx is not None and rows_idx < len(r):
                        try:
                            rows_total += int(float(str(r[rows_idx]).strip() or "0"))
                        except Exception:
                            pass
    except Exception:
        pass

    return {"date": date_str, "csv_count": csv_count, "rows_total": rows_total}

def latest_zip_names() -> list:
    zips = sorted(glob.glob(os.path.join(DIST, "*.zip")))
    # Nos quedamos con los de la misma marca temporal que el último manifest si existe
    latest = zips[-3:]  # fallback
    return [os.path.basename(p) for p in latest]

def read_drive_links() -> list:
    path = os.path.join(DIST, "drive_links.txt")
    lines = [x for x in read_text(path).splitlines() if x.strip()]
    # esperamos 3 líneas (legales, loterias, marketing) pero mostramos las que haya
    return lines

def parse_dq() -> dict:
    path = os.path.join(DIST, "dq_report.txt")
    txt = read_text(path)
    status = "UNKNOWN"
    warn = 0
    fail = 0
    # Buscar "Data Quality → OK|WARN|FAIL" o líneas similares
    m = re.search(r"Data Quality\s*[^\n]*\s*→\s*(OK|WARN|FAIL)", txt, re.IGNORECASE)
    if m:
        status = m.group(1).upper()
    # Buscar "(warn=X, fail=Y)"
    m2 = re.search(r"\(warn\s*=\s*(\d+)\s*,\s*fail\s*=\s*(\d+)\)", txt, re.IGNORECASE)
    if m2:
        warn = int(m2.group(1))
        fail = int(m2.group(2))
    return {"status": status, "warn": warn, "fail": fail, "raw": txt}

def find_master_csv() -> str:
    p = os.path.join(DIST, "loterias_master.csv")
    return os.path.basename(p) if os.path.exists(p) else ""

def find_latest_manifest() -> str:
    manifests = find_manifests()
    return os.path.basename(manifests[-1]) if manifests else ""

def now_utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

# ------------------------ construcción del reporte -------------------
def build_report():
    manifests = find_manifests()
    series = [parse_manifest_rows(p) for p in manifests]

    dq = parse_dq()
    latest_manifest = find_latest_manifest()
    master_csv = find_master_csv()
    zips = latest_zip_names()
    drive_links = read_drive_links()

    data = {
        "generated_at": now_utc_iso(),
        "latest_manifest": latest_manifest,
        "master_csv": bool(master_csv),
        "zips": zips,
        "drive_links": drive_links,
        "dq": dq,                 # {status, warn, fail, raw}
        "series": series,         # [{date,csv_count,rows_total}, ...]
    }
    return data

# ------------------------ HTML (Chart.js) ----------------------------
def render_html(report: dict) -> str:
    # Datos serie
    dates = [s["date"] for s in report["series"]]
    rows_tot = [s.get("rows_total", 0) for s in report["series"]]
    csv_cnt = [s.get("csv_count", 0) for s in report["series"]]

    dq = report["dq"]
    status = dq.get("status","UNKNOWN")
    badge = {"OK":"✅ OK", "WARN":"⚠️ WARN", "FAIL":"❌ FAIL"}.get(status, "❓ UNKNOWN")

    latest_manifest = html.escape(report.get("latest_manifest","—"))
    master_yes = "sí" if report.get("master_csv") else "no"
    zips_html = "\n".join(f"<code>{html.escape(z)}</code>" for z in report.get("zips", []))
    links_html = "\n".join(f'<a href="{html.escape(u)}" target="_blank">{html.escape(u.split("/")[-1])}</a>'
                           for u in report.get("drive_links", []))

    # HTML mínimo con Chart.js desde CDN
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Fran Ops · Data Quality</title>
  <link rel="preconnect" href="https://cdn.jsdelivr.net" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body{{font-family: system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Helvetica,Arial,sans-serif; margin:24px;}}
    h1{{margin:0 0 8px 0}}
    .meta{{color:#666;margin-bottom:18px}}
    section{{margin:18px 0}}
    .card{{border:1px solid #e5e7eb;border-radius:8px;padding:16px}}
    code{{background:#f6f8fa;padding:2px 6px;border-radius:4px}}
    .grid{{display:grid; gap:16px}}
    @media(min-width:900px){{ .grid.two{{grid-template-columns:1fr 1fr}} }}
    canvas{{max-width:100%;}}
    .badge{{display:inline-block;padding:4px 8px;border-radius:14px;background:#f1f5f9}}
  </style>
</head>
<body>
  <h1>Fran Ops · Data Quality</h1>
  <div class="meta">Actualizado: {html.escape(report.get("generated_at",""))}</div>

  <section class="card">
    <h2>Estado DQ</h2>
    <p>Estado: <span class="badge">{badge}</span>
      <small>(warn={dq.get("warn",0)}, fail={dq.get("fail",0)})</small>
    </p>
    <details><summary>Ver informe completo</summary>
      <pre style="white-space:pre-wrap">{html.escape(dq.get("raw",""))}</pre>
    </details>
  </section>

  <section class="card">
    <h2>Archivos</h2>
    <p>Manifest: <code>{latest_manifest}</code><br/>Master CSV: {master_yes}</p>
    <h3>ZIPs</h3>
    <div>{zips_html or "—"}</div>
    <h3>Enlaces de Drive</h3>
    <div>{links_html or "—"}</div>
  </section>

  <section class="grid two">
    <div class="card">
      <h3>Evolución · Total de filas (suma manifest)</h3>
      <canvas id="rowsChart"></canvas>
    </div>
    <div class="card">
      <h3>Evolución · Nº de CSVs en manifest</h3>
      <canvas id="csvChart"></canvas>
    </div>
  </section>

<script>
const labels = {json.dumps(dates)};
const rowsData = {json.dumps(rows_tot)};
const csvData = {json.dumps(csv_cnt)};

function buildLineChart(ctx, labels, data, label){
  return new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: labels,
      datasets: [{{
        label: label,
        data: data,
        tension: 0.25,
        pointRadius: 2
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      scales: {{
        x: {{ ticks: {{ maxRotation: 0, autoSkip: true }} }},
        y: {{ beginAtZero: true }}
      }},
      plugins: {{
        legend: {{ display: true }},
        tooltip: {{ mode: 'index', intersect: false }}
      }}
    }}
  }});
}

buildLineChart(document.getElementById('rowsChart'), labels, rowsData, 'Filas totales');
buildLineChart(document.getElementById('csvChart'), labels, csvData, 'CSV en manifest');
</script>

</body>
</html>
"""

# ------------------------ main --------------------------------------
def main():
    os.makedirs(DIST, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

    report = build_report()

    # report.json
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # docs/index.html
    html_str = render_html(report)
    with open(HTML_OUT, "w", encoding="utf-8") as f:
        f.write(html_str)

    print(f"✓ JSON: {REPORT_JSON}")
    print(f"✓ HTML: {HTML_OUT}")

if __name__ == "__main__":
    main()
