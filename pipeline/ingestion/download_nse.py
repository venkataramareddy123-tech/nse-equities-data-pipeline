"""Download all NSE raw files: cm bhavcopy zips (2015-2024-07), UDiFF (2024-07+),
MTO delivery (2015+), sec_bhavdata_full (2019-09-30+). Resumable."""
import datetime as dt
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# pyrefly: ignore [missing-import]
from dl import NSE_H, business_days, run_jobs

ROOT = os.path.join(_ROOT, "data", "raw", "nse")
# Rolling: today defaults to current date; override with ROLL_TODAY=YYYY-MM-DD.
TODAY = dt.date.fromisoformat(os.environ["ROLL_TODAY"]) if os.environ.get("ROLL_TODAY") else dt.date.today()
MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
       "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
UDIFF_START = dt.date(2024, 7, 8)
FULL_START = dt.date(2019, 9, 30)


def build_jobs():
    jobs = []
    for d in business_days(dt.date(2015, 1, 1), TODAY):
        y, m, day = d.year, d.month, d.day

        # Common delivery archive for all dates
        mto_url = f"https://archives.nseindia.com/archives/equities/mto/MTO_{d:%d%m%Y}.DAT"
        mto_path = os.path.join(ROOT, "mto", str(y), f"MTO_{d:%d%m%Y}.DAT")

        # --- ERA 1: 2015-01-01 to 2019-09-29 (Old Bhavcopy + MTO delivery) ---
        if d < FULL_START:
            cm_url = (f"https://archives.nseindia.com/content/historical/EQUITIES/"
                      f"{y}/{MON[m-1]}/cm{day:02d}{MON[m-1]}{y}bhav.csv.zip")
            cm_path = os.path.join(ROOT, "cm", str(y), f"cm{day:02d}{MON[m-1]}{y}bhav.csv.zip")
            jobs.append((cm_url, cm_path, NSE_H, b"PK"))
            jobs.append((mto_url, mto_path, NSE_H, None))

        # --- ERA 2: 2019-09-30 to 2024-07-07 (Old Bhavcopy + MTO + sec_bhavdata_full) ---
        elif d < UDIFF_START:
            cm_url = (f"https://archives.nseindia.com/content/historical/EQUITIES/"
                      f"{y}/{MON[m-1]}/cm{day:02d}{MON[m-1]}{y}bhav.csv.zip")
            cm_path = os.path.join(ROOT, "cm", str(y), f"cm{day:02d}{MON[m-1]}{y}bhav.csv.zip")
            full_url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
            full_path = os.path.join(ROOT, "full", str(y), f"sec_bhavdata_full_{d:%d%m%Y}.csv")
            jobs.append((cm_url, cm_path, NSE_H, b"PK"))
            jobs.append((mto_url, mto_path, NSE_H, None))
            jobs.append((full_url, full_path, NSE_H, None))

        # --- ERA 3: 2024-07-08 to Present (UDiFF Bhavcopy + MTO + sec_bhavdata_full) ---
        else:
            udiff_url = (f"https://nsearchives.nseindia.com/content/cm/"
                         f"BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip")
            udiff_path = os.path.join(ROOT, "udiff", str(y), f"BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip")
            full_url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
            full_path = os.path.join(ROOT, "full", str(y), f"sec_bhavdata_full_{d:%d%m%Y}.csv")
            jobs.append((udiff_url, udiff_path, NSE_H, b"PK"))
            jobs.append((mto_url, mto_path, NSE_H, None))
            jobs.append((full_url, full_path, NSE_H, None))

    return jobs


if __name__ == "__main__":
    jobs = build_jobs()
    print(f"{len(jobs)} NSE files to ensure", flush=True)
    res = run_jobs(jobs, workers=24, manifest_path=os.path.join(ROOT, "..", "manifest_nse.csv"))
    ok = sum(1 for r in res if r[2] in ("ok", "have"))
    fail = sum(1 for r in res if r[2] == "fail")
    if len(res) < len(jobs) or (fail and (ok == 0 or fail / max(len(res), 1) > 0.5)):
        print(f"ERROR: Download incomplete or failed ({fail} failed, {len(res)}/{len(jobs)} processed)", flush=True)
        sys.exit(1)
