"""Fetch NSE corporate actions history (monthly chunks) via curl_cffi impersonation.
Also grabs NSE symbol change list if available.

Cache discipline: a cached chunk only counts as good if
it parses as JSON *and* is a list — NSE error objects ({...} with HTTP 200) and
truncated writes are re-fetched, never trusted. Writes are atomic (tmp +
os.replace) so a killed process cannot leave a poisonable half-file behind.
"""
import datetime as dt
import json
import os
import sys
import time

# pyrefly: ignore [missing-import]
from curl_cffi import requests as cr

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

ROOT = os.path.join(_ROOT, "data", "raw", "nse", "corp_actions")


def month_chunks(start_year, end_year, end_month):
    chunks = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            if y == end_year and m > end_month:
                break
            last = 31
            if m in (4, 6, 9, 11):
                last = 30
            elif m == 2:
                last = 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28
            chunks.append((dt.date(y, m, 1), dt.date(y, m, last)))
    return chunks


def cached_ok(path):
    """True only for a parseable JSON list >100 bytes (self-heals bad caches)."""
    try:
        if not os.path.exists(path) or os.path.getsize(path) <= 100:
            return False
        with open(path, encoding="utf-8") as f:
            return isinstance(json.load(f), list)
    except Exception:  # noqa: BLE001 - any read/parse failure = refetch
        return False


def fetch_all():
    os.makedirs(ROOT, exist_ok=True)
    s = cr.Session(impersonate="chrome")
    s.get("https://www.nseindia.com/", timeout=30)
    # Rolling: fetch through current month; override end with ROLL_TODAY=YYYY-MM-DD.
    _today = dt.date.fromisoformat(os.environ["ROLL_TODAY"]) if os.environ.get("ROLL_TODAY") else dt.date.today()
    chunks = month_chunks(2014, _today.year, _today.month)
    ok = miss = 0
    for i, (fd, td) in enumerate(chunks):
        path = os.path.join(ROOT, f"ca_{fd:%Y%m%d}_{td:%Y%m%d}.json")
        if cached_ok(path):
            ok += 1
            continue
        url = (f"https://www.nseindia.com/api/corporates-corporateActions"
               f"?index=equities&from_date={fd:%d-%m-%Y}&to_date={td:%d-%m-%Y}")
        for attempt in range(3):
            try:
                r = s.get(url, timeout=45)
                d = r.json()
                if not isinstance(d, list):
                    raise ValueError(f"unexpected payload type {type(d).__name__}")
                tmp = path + ".part"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(d, f)
                os.replace(tmp, path)
                ok += 1
                break
            except Exception as e:
                time.sleep(2 * (attempt + 1))
                if attempt == 2:
                    print(f"FAIL {fd:%Y%m}: {type(e).__name__}", flush=True)
                    miss += 1
                else:
                    try:
                        s = cr.Session(impersonate="chrome")
                        s.get("https://www.nseindia.com/", timeout=30)
                    except Exception:
                        pass
        if (i + 1) % 12 == 0:
            print(f"[{i+1}/{len(chunks)}] ok={ok} miss={miss}", flush=True)
            try:
                s = cr.Session(impersonate="chrome")
                s.get("https://www.nseindia.com/", timeout=30)
            except Exception:
                pass
    print(f"DONE ok={ok} miss={miss}", flush=True)
    if miss and ok == 0:
        print("ERROR: every corporate-actions chunk failed -- network or block problem", flush=True)
        return False
    if miss:
        print(f"WARNING: {miss} chunk(s) failed; they retry on the next run", flush=True)
    return True


if __name__ == "__main__":
    ok = fetch_all()
    sym_ok = True
    # symbol change list (best effort)
    for u in ["https://nsearchives.nseindia.com/content/misc/symbol_change.csv"]:
        try:
            s = cr.Session(impersonate="chrome")
            r = s.get(u, timeout=30)
            out = os.path.join(ROOT, "symbol_change.csv")
            if r.status_code == 200 and len(r.content) > 500:
                tmp = out + ".part"
                open(tmp, "wb").write(r.content)
                os.replace(tmp, out)
                print("symbol_change.csv:", len(r.content), "bytes")
            else:
                print("symbol_change:", r.status_code, len(r.content))
        except Exception as e:
            print("symbol_change ERR", type(e).__name__)
    sys.exit(0 if (ok and sym_ok) else 1)
