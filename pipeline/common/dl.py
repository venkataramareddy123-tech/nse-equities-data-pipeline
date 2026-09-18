"""Generic threaded, resumable downloader with manifest logging."""
import concurrent.futures as cf
import csv
import datetime as dt
import os
import random
import threading
import time

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
NSE_H = {
    "User-Agent": UA, "Referer": "https://www.nseindia.com/", "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate",
}

LOCK = threading.Lock()


def business_days(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


def download_one(url, path, headers, expect_magic=None, min_bytes=800, retries=3):
    """Returns (status, bytes). status in ok, have, miss, fail.

    404 -> miss (holiday / not yet published). 403/429/5xx and network
    errors are retried with backoff then reported as fail: a bot-block
    must never be recorded as a holiday, which would blind the
    holiday-vs-missing digest the health check relies on.
    """
    if os.path.exists(path) and os.path.getsize(path) >= min_bytes:
        if expect_magic is None:
            return "have", os.path.getsize(path)
        with open(path, "rb") as f:
            if f.read(len(expect_magic)).startswith(expect_magic):
                return "have", os.path.getsize(path)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=45)
            if r.status_code == 200 and len(r.content) >= min_bytes:
                if expect_magic and not r.content[:len(expect_magic)].startswith(expect_magic):
                    return "miss", len(r.content)
                low = r.content[:512].lower()
                if b"<!doctype" in low or b"<html" in low:
                    return "miss", len(r.content)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = path + ".part"
                with open(tmp, "wb") as f:
                    f.write(r.content)
                os.replace(tmp, path)
                return "ok", len(r.content)
            if r.status_code == 404:
                return "miss", 0
            # 403/429/5xx: likely throttle or block -- back off and retry
        except Exception:
            pass
        time.sleep(1.5 * (attempt + 1) + random.random())
    return "fail", 0


def run_jobs(jobs, workers, manifest_path, max_consecutive_fails=20):
    """jobs: list of (url, path, headers, magic). Writes manifest CSV rows incrementally."""
    results = []
    done = [0]
    consecutive_fails = [0]
    abort_event = threading.Event()
    t0 = time.time()

    os.makedirs(os.path.dirname(manifest_path) or ".", exist_ok=True)
    manifest_f = open(manifest_path, "w", newline="", encoding="utf-8")
    w = csv.writer(manifest_f)
    w.writerow(["url", "path", "status", "bytes"])
    manifest_f.flush()

    def _job(j):
        url, path, headers, magic = j
        if abort_event.is_set():
            return None

        st, size = download_one(url, path, headers, magic)
        with LOCK:
            done[0] += 1
            if st in ("ok", "have"):
                consecutive_fails[0] = 0
            elif st == "fail":
                consecutive_fails[0] += 1
                if max_consecutive_fails and consecutive_fails[0] >= max_consecutive_fails:
                    if not abort_event.is_set():
                        abort_event.set()
                        print(
                            f"\n[!] CIRCUIT BREAKER TRIGGERED: {consecutive_fails[0]} consecutive failures. "
                            f"Aborting remaining downloads to avoid IP block/wasted retries.",
                            flush=True,
                        )

            w.writerow([url, path, st, size])
            manifest_f.flush()

            if done[0] % 50 == 0 or st in ("fail",):
                el = time.time() - t0
                print(f"[{done[0]}/{len(jobs)}] {el:7.0f}s  {st:5s} {os.path.basename(path)}", flush=True)
        return (url, path, st, size)

    try:
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_job, j) for j in jobs]
            for fut in cf.as_completed(futures):
                if abort_event.is_set():
                    for f in futures:
                        f.cancel()
                try:
                    if fut.cancelled():
                        continue
                    res = fut.result()
                    if res is not None:
                        results.append(res)
                except Exception:
                    pass
    finally:
        manifest_f.close()

    from collections import Counter
    print("SUMMARY", Counter(r[2] for r in results), f"{time.time()-t0:.0f}s total")
    return results
