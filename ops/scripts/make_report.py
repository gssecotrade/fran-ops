#!/usr/bin/env python3
# ops/scripts/make_report.py
import os, glob, json, time, re
from datetime import datetime, timezone
from string import Template
from pathlib import Path

# --- Config ---
DIST = os.environ.get("DIST_DIR", "dist")
DOCS_HTML = "docs/index.html"
DQ_REPORT_TXT = os.path.join(DIST, "dq_report.txt")
DRIVE_LINKS_TXT = os.path.join(DIST, "drive_links.txt")
REPORT_JSON = os.path.join(DIST, "report.json")

def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except:
        return ""

def find_latest_manifest():
    files = sorted(glob.glob(os.path.join(DIST, "loterias_manifest_*.csv")), reverse=True)
    return files[0] if files else ""

def drive_links():
    out = []
    if not os.path.exists(DRIVE_LINKS_TXT):
        return out
    with open(DRIVE_LINKS_TXT, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: 
                continue
            # Formato esperado: "<nombre>\t<url>"
            parts = line.split("\t")
            if len(parts) == 2:
                out.append({"name": parts[0], "url": parts[1]})
            else:
                # fallback: intenta detectar URL
                m = re.search(r"(https?://\S+)", line)
                url = m.group(1) if m else "#"
                out.append({"name": line.replace(url,"").strip() or "link", "url": url})
    return out

def zip_list():
    zips = sorted(glob.glob(os.path.join(DIST, "*.zip")))
    return [os.path.basename(z) for z in zips]

def parse_dq_status(txt):
    # Busca línea “Data Quality → WARN (warn=2, fail=0)” o similar
    status = "UNKNOWN"
    warn = fail = 0
    m = re.search(r"Data Quality\s*[^\n]*\s*→\s*([A-Z]+)\s*\(warn\s*=\s*(\d+),\s*fail\s*=\s*(\d+)\)", txt, re.I)
    if m:
        status = m.group(1).upper()
        warn = int(m.group(2))
        fail = int(m.group(3))
    else:
        # otra variante sin flecha:
        m2 = re.search(r"Data Quality\s*[^\n]*:\s*([A-Z]+)\s*\(warn\s*=\s*(\d+),\s*fail\s*=\s*(\d+)\)", txt, re.I)
        if m2:
            status = m2.group(1).upper()
            warn = int(m2.group(2))
            fail = int(m2.group(3))
    return status, warn, fail

def status_badge(status):
    s = (status or "").upper()
    if s == "OK":
        return '✅ OK'
    if s == "WARN":
        return '⚠️ WARN'
    if s == "FAIL":
        return '❌ FAIL'
    return 'ℹ️ UNKNOWN'

def build_report_json():
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    manifest = find_latest_manifest()
    manifest_name = os.path.basename(manifest) if manifest else ""
    master_exists = os.path.exists(os.path.join(DIST, "loterias_master.csv"))

    dq_txt = read_text(DQ_REPORT_TXT)
    dq_status, dq_warn, dq_fail = parse_dq_status(dq_txt)

    report = {
        "updated_utc": now_utc,
        "dq": {
            "status": dq_status,
            "warn": dq_warn,
            "fail": dq_fail
        },
        "files": {
            "manifest": manifest_name,
            "master_csv": bool(master_exists),
            "zips": zip_list()
        },
        "drive_links": drive_links()
    }
    os.makedirs(DIST, exist_ok=True)
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report

def as_html_list(items):
    return "\n".join([f"<li><code>{x}</code></li>" for x in items]) if items else "<li><em>(vacío)</em></li>"

def as_links_list(links):
    if not links:
        return "<li><em>(sin enlaces)</em></li>"
    out = []
    for l in links:
        name = (l.get("name") or "link").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        url = l.get("url") or "#"
        out.append(f'<li><a href="{url}" target="_blank" rel="noopener">{name}</a></li>')
    return "\n".join(out)

def build_chart_config(report):
    # Gráfica simple: nº de zips por categoría + warn/fail
    labels = ["ZIPs", "WARN", "FAIL"]
    data = [
        len(report["files"]["zips"]),
        int(report["dq"]["warn"] or 0),
        int(report["dq"]["fail"] or 0),
    ]
    cfg = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": "Resumen",
                "data": data
            }]
        },
        "options": {
            "responsive": True,
            "plugins": {
                "legend": { "display": True }
            },
            "scales": {
                "y": { "beginAtZero": True }
            }
        }
    }
    return json.dumps(cfg, ensure_ascii=False)

def build_html(report):
    updated = report["updated_utc"]
    badge = status_badge(report["dq"]["status"])
    manifest = report["files"]["manifest"] or "—"
    master_yes = "sí" if report["files"]["master_csv"] else "no"
    zips_html = as_html_list(report["files"]["zips"])
    links_html = as_links_list(report["drive_links"])
    chart_config = build_chart_config(report)

    # Usamos string.Template para evitar conflictos de llaves con JS
    tpl = Template("""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Fran Ops · Data Quality</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>
<style>
 body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:20px;color:#111}
 h1{font-size:28px;margin:0 0 6px}
 .muted{color:#666}
 section{margin:18px 0;padding:16px;border:1px solid #eee;border-radius:8px}
 code{background:#f6f8fa;padding:2px 6px;border-radius:4px}
 details{margin-top:8px}
 .badge{display:inline-block;padding:4px 10px;border-radius:999px;background:#efefef}
</style>
</head>
<body>
  <h1>Fran Ops · Data Quality</h1>
  <div class="muted">Actualizado: <strong>$updated</strong></div>

  <section>
    <h2>Estado DQ</h2>
    <p>Estado: <span class="badge">$badge</span> <span class="muted">(warn=$warn, fail=$fail)</span></p>
    <canvas id="chart" width="600" height="260"></canvas>
  </section>

  <section>
    <h2>Archivos</h2>
    <p>Manifest: <code>$manifest</code><br/>Master CSV: <strong>$master_yes</strong></p>
    <h3>ZIPs</h3>
    <ul>
      $zips_html
    </ul>
  </section>

  <section>
    <h2>Enlaces de Drive</h2>
    <ul>
      $links_html
    </ul>
  </section>

  <details>
    <summary>Ver JSON completo</summary>
    <pre><code>$report_pretty</code></pre>
  </details>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script>
    (function(){
      const ctx = document.getElementById('chart').getContext('2d');
      const cfg = $chart_config;
      new Chart(ctx, cfg);
    })();
  </script>
</body>
</html>""")

    html = tpl.substitute(
        updated=updated,
        badge=badge,
        warn=report["dq"]["warn"],
        fail=report["dq"]["fail"],
        manifest=manifest,
        master_yes=master_yes,
        zips_html=zips_html,
        links_html=links_html,
        report_pretty=json.dumps(report, ensure_ascii=False, indent=2),
        chart_config=chart_config
    )
    Path("docs").mkdir(parents=True, exist_ok=True)
    with open(DOCS_HTML, "w", encoding="utf-8") as f:
        f.write(html)

def main():
    report = build_report_json()
    build_html(report)
    print(f"✓ JSON: {REPORT_JSON}")
    print(f"✓ HTML: {DOCS_HTML}")

if __name__ == "__main__":
    main()
