#!/usr/bin/env python3
# ops/scripts/make_report.py
import json, glob, os, time, re
from datetime import datetime

DIST = os.environ.get("DIST_DIR", "dist")

def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except:
        return ""

def find_latest_manifest():
    files = sorted(glob.glob(os.path.join(DIST, "loterias_manifest_*.csv")), reverse=True)
    return files[0] if files else ""

def read_drive_links():
    path = os.path.join(DIST, "drive_links.txt")
    out = []
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            if "\t" in line:
                name, url = line.split("\t", 1)
                out.append({"name": name, "url": url})
            elif "http" in line:
                # fallback simple
                m = re.search(r"(https?://\S+)", line)
                url = m.group(1) if m else ""
                out.append({"name": line.replace(url,"").strip(" -·"), "url": url})
    return out

def main():
    dq = read_text(os.path.join(DIST, "dq_report.txt"))
    manifest = find_latest_manifest()
    master_exists = os.path.exists(os.path.join(DIST, "loterias_master.csv"))
    drive_links = read_drive_links()

    report = {
        "generated_utc": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "runner": os.environ.get("GITHUB_RUN_ID",""),
        "status": "OK" if "FAIL" not in dq else "FAIL",
        "dq_report": dq,
        "manifest_path": manifest.split("/")[-1] if manifest else "",
        "master_csv": "loterias_master.csv" if master_exists else "",
        "drive_links": drive_links,
    }
    os.makedirs(DIST, exist_ok=True)
    out_path = os.path.join(DIST, "report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("Wrote", out_path)

if __name__ == "__main__":
    main()
