# ops/scripts/fetch_lae_common.py
import re, datetime

def norm_date(s):
    s = s.strip()
    # acepta 25/09/2025 o 2025-09-25
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        d, mth, y = m.groups()
        return f"{y}-{mth}-{d}"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s[:10]
    return s  # última bala: devuélvela como venga

def today_utc():
    return datetime.datetime.utcnow().isoformat() + "Z"

def to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None
