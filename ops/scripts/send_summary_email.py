import os, glob, smtplib, socket
from email.utils import parseaddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# Ruta base del proyecto
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Credenciales que recogerá de los Secrets de GitHub
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TO   = os.getenv("SMTP_TO", os.getenv("SUMMARY_EMAIL", SMTP_USER))

def build_summary():
    """Genera el contenido del resumen a enviar por email"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"Resumen Fran Ops · {now}\n")

    # ZIPs generados en ./dist
    dist = os.path.join(BASE, "dist")
    zips = sorted(glob.glob(os.path.join(dist, "*.zip")))
    lines.append(f"ZIPs en dist/: {len(zips)}")
    for z in zips[:20]:
        lines.append(f"  - {os.path.basename(z)}")

    # Manifest en legales/salida
    legales = os.path.join(BASE, "legales", "salida")
    manifests = sorted(glob.glob(os.path.join(legales, "manifest_*.csv")))
    lines.append(f"\nÚltimo manifest: {os.path.basename(manifests[-1]) if manifests else '—'}")

    # CSVs en loterias/data
    lot = os.path.join(BASE, "loterias", "data")
    csvs = sorted(glob.glob(os.path.join(lot, "*.csv")))
    lines.append(f"\nCSVs en loterias/data/: {len(csvs)}")
    for c in csvs[-5:]:
        lines.append(f"  - {os.path.basename(c)}")

    return "\n".join(lines)

def send_email(subject, body):
    """Envía el email por SMTP"""
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

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(envelope_from, envelope_to, msg.as_string())
            print("📧 Email de resumen enviado (SMTP).")
    except Exception as e:
        print(f"❌ Error al enviar email: {e}")

if __name__ == "__main__":
    subject = f"[Fran Ops] Resumen {datetime.now().strftime('%Y-%m-%d %H:%M')} · {socket.gethostname()}"
    body = build_summary()
    print("=== RESUMEN ===\n" + body + "\n================")
    send_email(subject, body)
