#!/usr/bin/env python3
# ops/scripts/make_report.py
import os, glob, json, re
from datetime import datetime

# --- Paths (raíz del repo) ---
HERE = os.path.abspath(os.path.dirname(__file__))                # .../ops/scripts
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))          # raíz del repo
DIST = os.path.join(REPO, os.environ.get("DIST_DIR", "dist"))   # dist/ en raíz
DOCS = os.path.join(REPO, "docs")                               # docs/ en raíz

os.makedirs(DIST, exist_ok=True)
os.makedirs(DOCS, exist_ok=True)

# --- Utilidades ---
def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""

def find_latest_manifest():
    files = sorted(glob.glob(os.path.join(DIST, "loterias_manifest_*.csv")), reverse=True)
    return files[0] if files else ""

def read_drive_links():
    out = []
    path = os.path.join(DIST, "drive_links.txt")
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Formatos posibles:
            # "↑ Subido: nombre.zip  → https://..."
            # " - nombre.zip<TAB>https://..."
            m = re.match(r"[-•]?\s*(?P<name>.+?\.zip)\s*(?:→|\t|,)\s*(?P<url>https?://\S+)", line)
            if m:
                out.append({"name": m.group("name"), "url": m.group("url")})
    return out

def load_dq():
    path = os.path.join(DIST, "dq_report.txt")
    txt = read_text(path)
    status = "OK"
    warn = 0
    fail = 0
    if "FAIL" in txt or "Error" in txt or "✖" in txt:
        status = "FAIL"
    elif "WARN" in txt or "⚠" in txt:
        status = "WARN"
    # Intento sacar contadores si están impresos como (warn=X, fail=Y)
    m = re.search(r"\(warn\s*=\s*(\d+),\s*fail\s*=\s*(\d+)\)", txt)
    if m:
        warn = int(m.group(1))
        fail = int(m.group(2))
    return {"raw": txt, "status": status, "warn": warn, "fail": fail}

# --- Datos para el panel/JSON ---
ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
manifest = find_latest_manifest()
master_csv = os.path.join(DIST, "loterias_master.csv")
drive_links = read_drive_links()
dq = load_dq()
zips = sorted(glob.glob(os.path.join(DIST, "*.zip")))
zips_basename = [os.path.basename(z) for z in zips]

payload = {
    "timestamp_utc": ts,
    "manifest": os.path.basename(manifest) if manifest else "",
    "master_exists": os.path.exists(master_csv),
    "drive_links": drive_links,
    "zips": zips_basename,
    "dq": dq,
}

# --- Escribe JSON ---
json_path = os.path.join(DIST, "report.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)

# --- Render HTML muy simple (lee JSON vía <script> embebido para no depender de rutas relativas en Pages) ---
html = f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Fran Ops · Data Quality</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:20px;}}
.card{{border:1px solid #e5e7eb;border-radius:8px;padding:16px;margin:12px 0}}
.badge{{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px}}
.ok{{background:#e6ffed;border:1px solid #2ea04333;color:#1a7f37}}
.warn{{background:#fff7e6;border:1px solid #f2c03766;color:#9a6700}}
.fail{{background:#ffe6e6;border:1px solid #d73a4933;color:#d1242f}}
code{{background:#f6f8fa;padding:2px 6px;border-radius:4px}}
ul{{margin:6px 0 0 18px}}
h1{{margin:0 0 12px 0}}
.small{{color:#6b7280;font-size:12px}}
</style>
</head>
<body>
<h1>Fran Ops · Data Quality</h1>
<p class="small">Actualizado: <b>{ts}</b></p>

<div class="card">
  <h2>Estado DQ</h2>
  <p>
    Estado: {{
      "OK": "<span class='badge ok'>✅ OK</span>",
      "WARN":"<span class='badge warn'>⚠️ WARN</span>",
      "FAIL":"<span class='badge fail'>❌ FAIL</span>"
    }}["{dq['status']}"]
    &nbsp;&nbsp;<span class="small">(warn={dq['warn']}, fail={dq['fail']})</span>
  </p>
  <details>
    <summary>Ver informe completo</summary>
    <pre style="white-space:pre-wrap">{dq['raw'].replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")}</pre>
  </details>
</div>

<div class="card">
  <h2>Archivos</h2>
  <p>Manifest: {"<code>"+os.path.basename(manifest)+"</code>" if manifest else "<i>No encontrado</i>"}<br/>
     Master CSV: {"<b>sí</b>" if os.path.exists(master_csv) else "<b>no</b>"}
  </p>
  <h3>ZIPs</h3>
  <ul>
    {"".join(f"<li><code>{name}</code></li>" for name in zips_basename) or "<i>Sin ZIPs</i>"}
  </ul>
</div>

<div class="card">
  <h2>Enlaces de Drive</h2>
  <ul>
    {"".join(f"<li><a target='_blank' href='{d['url']}'>{d['name']}</a></li>" for d in drive_links) or "<i>Sin enlaces</i>"}
  </ul>
</div>

</body>
</html>
"""

html_path = os.path.join(DOCS, "index.html")
with open(html_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"✓ JSON: {json_path}")
print(f"✓ HTML: {html_path}")
