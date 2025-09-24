import os, smtplib, ssl, socket
from email.mime.text import MIMEText
from email.utils import parseaddr
from dotenv import load_dotenv

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

HOST    = os.getenv("SMTP_HOST", "smtp.gmail.com")
PORT_TLS= int(os.getenv("SMTP_PORT", "587"))    # STARTTLS
PORT_SSL= 465                                   # SSL directo
USER    = os.getenv("SMTP_USER", "")
PASS    = os.getenv("SMTP_PASS", "")
FROM    = parseaddr(os.getenv("SMTP_FROM", USER))[1] or USER
TO      = [e.strip() for e in os.getenv("SMTP_TO", USER).split(",") if e.strip()]

subject = "[Fran Ops] Test SMTP"
body    = f"Prueba SMTP desde {socket.gethostname()}.\nFROM={FROM}\nTO={TO}\n"

def try_tls():
    print(f"→ TLS: conectando {HOST}:{PORT_TLS} como {USER}")
    s = smtplib.SMTP(HOST, PORT_TLS, timeout=20)
    s.set_debuglevel(1)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(USER, PASS)
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = FROM
    msg["To"] = ", ".join(TO)
    msg["Subject"] = subject + " (TLS)"
    resp = s.sendmail(FROM, TO, msg.as_string())
    s.quit()
    return resp

def try_ssl():
    print(f"→ SSL: conectando {HOST}:{PORT_SSL} como {USER}")
    ctx = ssl.create_default_context()
    s = smtplib.SMTP_SSL(HOST, PORT_SSL, context=ctx, timeout=20)
    s.set_debuglevel(1)
    s.login(USER, PASS)
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = FROM
    msg["To"] = ", ".join(TO)
    msg["Subject"] = subject + " (SSL)"
    resp = s.sendmail(FROM, TO, msg.as_string())
    s.quit()
    return resp

if not (USER and PASS and TO):
    raise SystemExit("⚠️ Falta SMTP_USER/SMTP_PASS/SMTP_TO en ops/.env")

try:
    r = try_tls()
    print("✅ TLS OK" if not r else f"⚠️ TLS respuesta parcial: {r}")
except Exception as e:
    print(f"❌ TLS error: {e}")
    try:
        r = try_ssl()
        print("✅ SSL OK" if not r else f"⚠️ SSL respuesta parcial: {r}")
    except Exception as e2:
        print(f"❌ SSL error: {e2}")
        raise SystemExit(1)

print("📧 Test enviado. Revisa Recibidos/Spam y la carpeta Enviados del Gmail.")
