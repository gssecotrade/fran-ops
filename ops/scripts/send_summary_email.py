#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, glob, smtplib, socket
from email.utils import parseaddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TO   = os.getenv("SMTP_TO", os.getenv("SUMMARY_EMAIL", SMTP_USER))

def tail(path, n=20):
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [l.rstrip() for l in f.readlines()]
        return lines[-n:]
    except Exception:
        return []

def build_summary():
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"Resumen Fran Ops · {now}\n")

    dist = os.path.join(BASE, "dist")
    zips = sorted(glob.glob(os.path.join(dist, "*.zip")))
    lines.append(f"ZIPs en dist/: {len(zips)}")
    for z in zips[:20]:
        lines.append(f"  - {os.path.basename(z)}")

    # Manifest
    manifests = sorted(glob.glob(os.path.join(dist, "loterias_manifest_*.csv")))
    lines.append(f"\nÚltimo manifest: {os.path.basename(manifests[-1]) if manifests else '—'}")

    # Master
    master_csv = os.path.join(dist, "loterias_master.csv")
    if os.path.exists(master_csv):
        lines.append("Master CSV: loterias_master.csv")

    # Drive links (si existen)
    drive_links = os.path.join(dist, "drive_links.txt")
    if os.path.exists(drive_links):
        links = tail(drive_links, 20)
        if links:
            lines.append("\nLinks Drive subidos:")
            for l in links[-10:]:
                lines.append("  - " + l)

    # CSVs de loterias/data
    lot = os.path.join(BASE, "loterias", "data")
    csvs = sorted(glob.glob(os.path.join(lot, "*.csv")))
    lines.append(f"\nCSVs en loterias/data/: {len(csvs)}")
    for c in csvs[-5:]:
        lines.append(f"  - {os.path.basename(c)}")

    return "\n".join(lines)

def send_email(subject, body):
    if not (SMTP_USER and SMTP_PASS and SMTP_TO):
        print("⚠️ Falta SMTP_USER/SMTP_PASS/SMTP_TO en secrets")
        return

    msg = MIMEMultipart()
    msg["From"] = SMTP_FROM
    msg["To"] = SMTP_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    envelope_from = parseaddr(SMTP_FROM)[1] or SMTP_USER
    envelope_to = [e.strip() for e in SMTP_TO.split(",") if e.strip()]

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(envelope_from, envelope_to, msg.as_string())
        print("📧 Email de resumen enviado (SMTP).")

if __name__ == "__main__":
    subject = f"[Fran Ops] Resumen {datetime.now().strftime('%Y-%m-%d %H:%M')} · {socket.gethostname()}"
    body = build_summary()
    print("=== RESUMEN ===\n" + body + "\n================")
    send_email(subject, body)
