#!/usr/bin/env python3
# ops/scripts/send_summary_email.py
import os, smtplib, json
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

DIST = os.environ.get("DIST_DIR", "dist")

def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except:
        return ""

def read_report_json():
    # make_report.py deja report.json en dist/ y el workflow lo copia a docs/
    # Preferimos dist/report.json; si no está, probamos docs/report.json
    candidates = [os.path.join(DIST, "report.json"), os.path.join("docs", "report.json")]
    for p in candidates:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return None

def build_body():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append("=== RESUMEN ===")
    lines.append(f"Resumen Fran Ops · {now} UTC")
    lines.append("")

    # Inventario de ZIPs
    try:
        items = sorted([f for f in os.listdir(DIST) if f.endswith(".zip")])
    except FileNotFoundError:
        items = []
    lines.append(f"ZIPs en dist/: {len(items)}")
    for z in items:
        lines.append(f"  - {z}")
    lines.append("")

    # Manifest y Master CSV si existen
    manifest = ""
    master   = ""
    try:
        manifest = sorted([f for f in os.listdir(DIST) if f.startswith("loterias_manifest_") and f.endswith(".csv")])[-1]
    except:
        pass
    if manifest:
        lines.append(f"Último manifest: {manifest}")
    else:
        lines.append("Último manifest: —")
    master_path = os.path.join(DIST, "loterias_master.csv")
    lines.append(f"Master CSV: {'sí' if os.path.exists(master_path) else 'no'}")
    lines.append("")

    # Data Quality (texto plano si existe)
    dq_path = os.path.join(DIST, "dq_report.txt")
    dq_text = read_text(dq_path)
    if dq_text:
        lines.append("--- DATA QUALITY ---")
        lines.append(dq_text)
        lines.append("")

    # Panel DQ (desde report.json)
    rpt = read_report_json()
    if rpt:
        status = rpt.get("status", "UNKNOWN")
        url    = rpt.get("page_url", "") or rpt.get("panel_url", "")
        emoji  = {"OK":"✅", "WARN":"⚠️", "FAIL":"❌"}.get(status, "ℹ️")
        lines.append("--- PANEL DQ ---")
        lines.append(f"Estado: {emoji} {status}")
        if url:
            lines.append(f"URL: {url}")
        lines.append("")
    else:
        lines.append("--- PANEL DQ ---")
        lines.append("No se encontró report.json (aún).")
        lines.append("")

    return "\n".join(lines)

def send_email(body: str):
    user = os.environ.get("SMTP_USER")
    pw   = os.environ.get("SMTP_PASS")
    to   = os.environ.get("SMTP_TO")
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))

    if not (user and pw and to):
        print("⚠️ Falta SMTP_USER/SMTP_PASS/SMTP_TO en secrets")
        print(body)
        return

    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"]   = to
    msg["Subject"] = f"[Fran Ops] Resumen {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"

    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, pw)
        server.send_message(msg)
    print("📧 Email de resumen enviado (SMTP).")

def main():
    body = build_body()
    print(body)
    send_email(body)

if __name__ == "__main__":
    main()
