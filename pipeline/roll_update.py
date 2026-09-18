"""Rolling one-click updater: fill missing bhavcopies -> rebuild -> self-check -> auto-rectify.

Usage (double-click UPDATE_DATA.bat, or):
    python pipeline/roll_update.py --auto            # full rolling update
    python pipeline/roll_update.py --check-only      # QA checks only, no downloads/rebuild

Order mirrors README: downloads -> CA fetch -> parse -> corp parse -> master
-> adjust -> QA -> health verdict. Downloads are resumable (skip existing);
parse/adjust rebuild from raw so re-runs also repair corruption.

Outputs:
    reports/health.json                 # machine-readable verdict (ok / repaired / attention)
    reports/data_quality_report.md      # from qa_report.py
    logs/roll_update_<ts>.log           # full run log
"""
import argparse
import datetime as dt
import glob
import json
import logging
import os
import shutil
import subprocess
import sys
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COMMON = os.path.join(HERE, "common")
for p in [HERE, COMMON, ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    from pipeline.common._util import atomic_write_json, kill_tree
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_json, kill_tree
BASE = os.path.join(ROOT, "data")
FINAL = os.path.join(BASE, "final")
REPORT = os.path.join(ROOT, "reports")
LOGDIR = os.path.join(ROOT, "logs")
os.makedirs(LOGDIR, exist_ok=True)

TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
LOGPATH = os.path.join(LOGDIR, f"roll_update_{TS}.log")
LOCKPATH = os.path.join(LOGDIR, ".roll_update.lock")
# Rebuild-in-progress marker: written when a rebuild starts, removed when it
# completes. The no-op fast path refuses to skip while it exists, so an
# aborted rebuild can never be papered over by a green "ok" verdict.
REBUILD_MARKER = os.path.join(LOGDIR, ".rebuild_pending")
log = logging.getLogger("roll")
log.addHandler(logging.NullHandler())  # import stays silent; main() wires real sinks


def setup_logging() -> None:
    """Wire console+file sinks. Deferred (not at import) so importing this
    module — tests, probes — never creates files nor binds stdio early."""
    os.makedirs(LOGDIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOGPATH, encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
        force=True,
    )

STEPS = [  # (label, script) in pipeline order
    ("download_nse", os.path.join("ingestion", "download_nse.py")),
    ("fetch_nse_ca", os.path.join("ingestion", "fetch_nse_ca.py")),
    ("parse_standardize", os.path.join("processing", "parse_standardize.py")),
    ("corp_actions_parse", os.path.join("processing", "corp_actions_parse.py")),
    ("build_master", os.path.join("processing", "build_master.py")),
    ("adjust", os.path.join("processing", "adjust.py")),
    ("market_tape", os.path.join("features", "market_tape.py")),
    ("row_flags", os.path.join("features", "row_flags.py")),
    ("qa_report", os.path.join("validation", "qa_report.py")),
]
REBUILD_STEPS = [
    os.path.join("processing", "parse_standardize.py"),
    os.path.join("processing", "corp_actions_parse.py"),
    os.path.join("processing", "build_master.py"),
    os.path.join("processing", "adjust.py"),
    os.path.join("features", "market_tape.py"),
    os.path.join("features", "row_flags.py"),
    os.path.join("validation", "qa_report.py"),
]


def run_step(script: str) -> bool:
    """Run one pipeline script, stream output to console+log. True on success."""
    log.info("=" * 60)
    log.info("STEP %s", script)
    try:
        env = dict(os.environ)
        # stream child output live: if this parent ever dies, the log still
        # shows exactly where the pipeline was (no more blind 10-min gaps).
        p = subprocess.Popen(
            [sys.executable, "-u", os.path.join(HERE, script)],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env)
        tail: list = []
        assert p.stdout is not None
        for line in p.stdout:
            line = line.rstrip()
            if line:
                log.info("[%s] %s", script, line)
                tail.append(line)
        code = p.wait(timeout=7200)
        if code != 0:
            log.error("%s FAILED (code %d, tail): %s", script, code,
                      " // ".join(tail[-8:])[-2000:])
            return False
        log.info("%s OK", script)
        return True
    except subprocess.TimeoutExpired:
        log.error("%s TIMEOUT after 2h -- killing process tree", script)
        kill_tree(p)
        return False
    except Exception as e:  # noqa: BLE001
        log.error("%s ERROR %s: %s", script, type(e).__name__, e)
        return False


def preflight() -> list:
    """Environment sanity. Returns list of problem strings (empty = fine)."""
    problems = []
    if sys.version_info < (3, 10):
        problems.append(f"python {sys.version} < 3.10")
    for mod in ("pandas", "pyarrow", "numpy", "requests"):
        try:
            __import__(mod)
        except ImportError:
            problems.append(f"missing dependency: {mod} (pip install {mod})")
    try:
        import curl_cffi  # noqa: F401  (needed by fetch_nse_ca)
    except ImportError:
        problems.append("missing dependency: curl_cffi (pip install curl_cffi)")
    free_gb = shutil.disk_usage(ROOT).free / 1e9
    if free_gb < 5:
        problems.append(f"disk free {free_gb:.1f}GB < 5GB")
    return problems


def newest_mtime(root: str) -> float:
    """Newest CONTENT file mtime under root (0.0 if missing/empty).

    Manifests are excluded: the downloader rewrites them on every run, and
    counting them kept the no-op fast path from ever triggering.
    """
    best = 0.0
    if not os.path.isdir(root):
        return best
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            if f.startswith("manifest_"):
                continue
            try:
                t = os.path.getmtime(os.path.join(dirpath, f))
                if t > best:
                    best = t
            except OSError:
                pass
    return best


_LOCK_FD = None  # held open (and locked) for the whole run


def acquire_lock() -> bool:
    """One updater at a time: concurrent runs corrupt shared parquet files.

    Holds an OS file lock for the process lifetime (msvcrt.locking on
    Windows, flock elsewhere). The OS releases it if we die, so there is no
    PID file to go stale and no PID-reuse false positives — the two flaws of
    pidfile designs (plus os.kill liveness probes misbehave on some Windows
    builds: WinError 87 on live processes).
    """
    global _LOCK_FD
    try:
        os.makedirs(os.path.dirname(LOCKPATH) or ".", exist_ok=True)
        fd = os.open(LOCKPATH, os.O_CREAT | os.O_WRONLY, 0o644)
    except OSError as e:
        log.error("Cannot create lock file %s: %s", LOCKPATH, e)
        return False
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        try:
            with open(LOCKPATH, encoding="utf-8") as f:
                holder = f.read().strip() or "?"
        except OSError:
            holder = "?"
        log.error("Another update (PID %s) is already running -- refusing to start. "
                  "Let it finish or close its window first.", holder)
        return False
    try:
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
    except OSError:
        pass
    _LOCK_FD = fd  # keep open: closing releases the lock
    return True


def release_lock() -> None:
    """Release the run lock (also runs via atexit)."""
    global _LOCK_FD
    try:
        if _LOCK_FD is not None:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(_LOCK_FD, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
            finally:
                os.close(_LOCK_FD)
                _LOCK_FD = None
        try:
            os.remove(LOCKPATH)
        except OSError:
            pass
    except OSError:
        pass


def download_digest() -> dict:
    """Classify manifest misses so holidays/lag don't look like data loss.

    holiday_like  = dates with price AND delivery missing (exchange shut)
    delivery_lag  = delivery-only misses within last 7 days (publishes late,
                    refills on later runs via the MTO<->full fallback chain)
    unexpected    = price-only misses (never seen in 11.7y; would page attention)
    """
    import re
    info = {"total_miss": 0, "holiday_like": 0, "delivery_lag": [],
            "unexpected_price_only": []}
    mp = os.path.join(BASE, "raw", "manifest_nse.csv")
    if not os.path.exists(mp):
        return info
    import csv
    MON = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05',
           'JUN': '06', 'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10',
           'NOV': '11', 'DEC': '12'}
    # NSE's 2026 equity calendar lists Ganesh Chaturthi on 2026-09-14.
    # Keep this explicit because the archive exposes a stale prior-session
    # full file for that date while the MTO endpoint fails.
    known_holidays = {'2026-09-14'}

    def fdate(path):
        b = path.replace('\\', '/').split('/')[-1]
        m = re.search(r'MTO_(\d{2})(\d{2})(\d{4})', b)
        if m:
            return f'{m.group(3)}-{m.group(2)}-{m.group(1)}'
        m = re.search(r'full_(\d{2})(\d{2})(\d{4})', b)
        if m:
            return f'{m.group(3)}-{m.group(2)}-{m.group(1)}'
        m = re.search(r'cm(\d{2})([A-Z]{3})(\d{4})', b)
        if m:
            return f'{m.group(3)}-{MON[m.group(2)]}-{m.group(1)}'
        m = re.search(r'_(\d{4})(\d{2})(\d{2})_F_0000', b)
        if m:
            return f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
        return ''

    price_miss, deliv_miss = set(), set()
    for r in csv.DictReader(open(mp, encoding='utf-8')):
        if r['status'] != 'miss':
            continue
        info['total_miss'] += 1
        p = r['path'].replace('\\', '/')
        d = fdate(r['path'])
        if '/cm/' in p or '/udiff/' in p:
            price_miss.add(d)
        else:
            deliv_miss.add(d)
    info['holiday_like'] = len((price_miss & deliv_miss)
                               | (price_miss & known_holidays))
    info['unexpected_price_only'] = sorted(
        price_miss - deliv_miss - known_holidays
    )
    today = dt.date.today()
    for d in sorted(deliv_miss - price_miss):
        try:
            y, mth, day = map(int, d.split('-'))
            age = (today - dt.date(y, mth, day)).days
            if age <= 7:
                info['delivery_lag'].append(f'{d} ({age}d ago, refills automatically)')
        except ValueError:
            pass
    return info


def find_corrupt_raw() -> list:
    """Scan raw files for truncated/corrupt content. Returns paths to delete.

    Zips (nse cm/udiff) get a full testzip; text sources (MTO .DAT,
    sec_bhavdata_full .csv) get a size floor plus an HTML/JSON
    error-page sniff -- an Akamai challenge saved before the guard existed
    must not be trusted forever.
    """
    bad = []
    for pat in [os.path.join(BASE, "raw", "nse", "cm", "*", "*"),
                os.path.join(BASE, "raw", "nse", "udiff", "*", "*")]:
        for p in glob.glob(pat):
            try:
                if not p.lower().endswith(".zip"):
                    continue
                if os.path.getsize(p) < 800:
                    bad.append(p)
                    continue
                with zipfile.ZipFile(p) as z:
                    if z.testzip() is not None:
                        bad.append(p)
            except Exception:  # noqa: BLE001 - any read failure = corrupt
                bad.append(p)
    for pat in [os.path.join(BASE, "raw", "nse", "mto", "*", "*"),
                os.path.join(BASE, "raw", "nse", "full", "*", "*")]:
        for p in glob.glob(pat):
            try:
                if p.lower().endswith(".zip"):
                    continue
                if os.path.getsize(p) < 800:
                    bad.append(p)
                    continue
                with open(p, "rb") as f:
                    head = f.read(512).lower()
                if b"<!doctype" in head or b"<html" in head or head.startswith(b"{"):
                    bad.append(p)
            except Exception:  # noqa: BLE001
                bad.append(p)
    return bad


def self_check() -> dict:
    """Lightweight QA verdict on final data. Returns {name: (ok, detail)}."""
    checks: dict = {}
    today = dt.date.today()
    try:
        import pandas as pd
    except ImportError:
        return {"imports": (False, "pandas missing")}

    nse_p = os.path.join(FINAL, "nse_equity_2015_2026.parquet")
    ok = os.path.exists(nse_p) and os.path.getsize(nse_p) > 0
    checks["nse_final_exists"] = (ok, f"{os.path.getsize(nse_p)/1e6:.0f}MB" if ok else "missing")

    # QA summary thresholds (written by qa_report.py)
    qap = os.path.join(REPORT, "qa_summary.json")
    if os.path.exists(qap):
        try:
            with open(qap, encoding="utf-8") as f:
                qa = json.load(f)
            nse = qa.get("NSE", {}).get("ohlc", {})
            bad_ohlc = sum(v for k, v in nse.items() if k not in ("volume==0 rows",))
            checks["nse_ohlc_clean"] = (bad_ohlc == 0, f"violations={bad_ohlc}")
            d = qa.get("NSE", {}).get("delivery", {})
            out = d.get("deliv_pct outside [0,100]", 0)
            checks["delivery_bounds"] = (out == 0, f"outside={out}")
            cov = float(d.get("deliv_pct coverage", 1.0))
            checks["delivery_coverage"] = (cov >= 0.95, f"coverage={cov:.4f}")
            cal = qa.get("NSE", {}).get("calendar", {})
            gap = cal.get("max_gap_days", 99)
            checks["calendar_sane"] = (gap <= 10, f"max_gap={gap}d last={cal.get('last')}")
            mm = qa.get("NSE", {}).get("prev_close_mismatch_gt_1pct", 0)
            tot = qa.get("NSE", {}).get("prev_close_pairs_checked", 1) or 1
            checks["prev_close_rate"] = (mm / tot < 0.02, f"{mm}/{tot} ({mm/tot:.2%})")
        except Exception as e:  # noqa: BLE001
            checks["qa_summary_parse"] = (False, f"{type(e).__name__}: {e}")
    else:
        checks["qa_summary_exists"] = (False, "qa_report.py did not produce it")

    # Per-file parse failures (parse_standardize writes fail counts per year).
    psp = os.path.join(BASE, "interim", "parse_status.json")
    if os.path.exists(psp):
        try:
            with open(psp, encoding="utf-8") as f:
                ps = json.load(f)
            cur = {k: v for k, v in ps.items() if k == str(today.year)}
            old = {k: v for k, v in ps.items() if k != str(today.year)}
            checks["parse_fails"] = (len(cur) == 0,
                                     f"current-year={cur or 'none'} older={old or 'none'}")
        except Exception as e:  # noqa: BLE001
            checks["parse_fails"] = (False, f"{type(e).__name__}: {e}")
    else:
        checks["parse_fails"] = (True, "no parse_status (no rebuild yet)")

    # Duplicate keys on (isin,date,series) must be zero
    try:
        if os.path.exists(nse_p):
            cols = ["isin", "date", "series", "symbol", "close", "turnover"]
            df = pd.read_parquet(nse_p, columns=cols)
            d3 = int(df.duplicated(["isin", "date", "series"]).sum())
            if d3 == 0:
                checks["nse_key_dupes"] = (True, "(isin,date,series) dupes=0")
            else:
                checks["nse_key_dupes"] = (False, f"(isin,date,series) dupes={d3}")
    except Exception as e:  # noqa: BLE001
        checks["dupe_check"] = (False, f"{type(e).__name__}: {e}")

    # Reconciliation: capital actions (cap_c != 1) within tolerance
    try:
        rp = os.path.join(FINAL, "corporate_actions_reconciliation.parquet")
        if os.path.exists(rp):
            r = pd.read_parquet(rp, columns=["cap_c", "recon_ok"])
            cap = r[r["cap_c"] != 1.0]
            rate = float(cap["recon_ok"].mean()) if len(cap) else 1.0
            checks["recon_capital"] = (rate >= 0.85, f"{rate:.1%} of {len(cap)} capital actions")
    except Exception as e:  # noqa: BLE001
        checks["recon_check"] = (False, f"{type(e).__name__}: {e}")

    # Freshness: last NSE session within ~6 calendar days (run at least weekly).
    try:
        if os.path.exists(nse_p):
            last = pd.read_parquet(nse_p, columns=["date"])["date"].max()
            last_d = pd.to_datetime(last).date()
            age = (today - last_d).days
            checks["freshness"] = (age <= 6, f"last={last_d} ({age}d ago)")
    except Exception as e:  # noqa: BLE001
        checks["freshness"] = (False, f"{type(e).__name__}: {e}")

    # Download misses: unexpected price gaps fail; holidays + lag are info.
    # A price miss pages only when it is recent (<=30d) and not today's file
    # (bhavcopy publishes in the evening; MTO lands first). Older mysteries
    # stay visible in the detail without blocking the verdict.
    try:
        dg = download_digest()
        unexp = dg["unexpected_price_only"]
        paging = []
        for ds in unexp:
            try:
                y, m, dd = map(int, ds.split("-"))
                age = (today - dt.date(y, m, dd)).days
                if 0 < age <= 30:
                    paging.append(ds)
            except ValueError:
                paging.append(ds)
        checks["unexpected_miss"] = (
            len(paging) == 0,
            f"holiday_like={dg['holiday_like']} lag={dg['delivery_lag'] or 'none'} "
            f"paging={paging or 'none'} info={unexp or 'none'}")
    except Exception as e:  # noqa: BLE001
        checks["unexpected_miss"] = (False, f"{type(e).__name__}: {e}")
    return checks


def write_health(checks: dict, status: str, actions: list) -> None:
    os.makedirs(REPORT, exist_ok=True)
    health = {
        "ts": TS, "status": status,  # ok | repaired | attention
        "checks": {k: {"ok": bool(v[0]), "detail": str(v[1])} for k, v in checks.items()},
        "actions_taken": actions,
        "log": os.path.basename(LOGPATH),
    }
    atomic_write_json(health, os.path.join(REPORT, "health.json"))
    bad = [k for k, v in checks.items() if not v[0]]
    log.info("HEALTH=%s bad=%s log=%s", status, bad, LOGPATH)


def attempt_repair(actions: list) -> bool:
    """Corrupt-raw scan -> delete -> re-download -> forced full rebuild.

    Returns True only when corrupt raw was found AND the rebuild completed;
    a failed rebuild with nothing corrupt to delete still returns False so
    the caller reports attention instead of papering over it.
    """
    corrupt = find_corrupt_raw()
    if not corrupt:
        return False
    for p in corrupt[:500]:
        try:
            os.remove(p)
        except OSError:
            pass
    actions.append(f"deleted {len(corrupt)} corrupt raw files for re-download")
    run_step(os.path.join("ingestion", "download_nse.py"))
    os.environ["ROLL_FULL"] = "1"  # repair rebuild must not skip years
    return all(run_step(s) for s in REBUILD_STEPS)


def main() -> int:
    ap = argparse.ArgumentParser(description="Rolling NSE updater with self-repair.")
    ap.add_argument("--auto", action="store_true", help="non-interactive (for .bat)")
    ap.add_argument("--check-only", action="store_true", help="QA checks only")
    ap.add_argument("--skip-downloads", action="store_true", help="skip download/fetch steps")
    ap.add_argument("--full", action="store_true",
                    help="force full rebuild even with no new data (same as ROLL_FULL=1)")
    args = ap.parse_args()
    actions: list = []
    forced = (args.full or os.environ.get("ROLL_FULL") == "1"
              or bool(os.environ.get("ROLL_FORCE_YEARS")))
    setup_logging()
    import atexit
    if not acquire_lock():
        return 3
    atexit.register(release_lock)

    probs = preflight()
    if probs:
        log.error("PREFLIGHT FAIL: %s", probs)
        write_health({"preflight": (False, "; ".join(probs))}, "attention", actions)
        return 2
    if args.check_only:
        checks = self_check()
        status = "ok" if all(v[0] for v in checks.values()) else "attention"
        write_health(checks, status, ["check-only"])
        return 0 if status == "ok" else 1

    # 1) downloads + CA fetch (resumable: existing files skipped)
    raw_before = newest_mtime(os.path.join(BASE, "raw"))
    for label, script in STEPS:
        if args.skip_downloads and label in ("download_nse", "fetch_nse_ca"):
            continue
        if "download" in label or "fetch" in label:
            if not run_step(script):
                log.error("Network step %s failed; continuing to rebuild from cache", script)
                actions.append(f"{script}: FAILED-continued")
        else:
            break

    # 1b) no-op fast path: nothing new on disk -> checks only, no rebuild.
    # New downloads/CA files bump raw mtimes; ROLL_FULL=1 / --full overrides.
    # A leftover rebuild marker (previous run died mid-rebuild) forces the
    # rebuild so mixed half-old finals can never pass as "ok".
    raw_after = newest_mtime(os.path.join(BASE, "raw"))
    finals_ok = all(os.path.exists(os.path.join(FINAL, f)) for f in
                    ("nse_equity_2015_2026.parquet", "corporate_actions.parquet"))
    qa_ok = os.path.exists(os.path.join(REPORT, "qa_summary.json"))
    if (not forced and raw_after <= raw_before and finals_ok and qa_ok
            and not os.path.exists(REBUILD_MARKER)):
        log.info("No new raw data since last run -- rebuild skipped (checks only)")
        actions.append("no-op fast path: raw unchanged, rebuild skipped")
        checks = self_check()
        status = "ok" if all(v[0] for v in checks.values()) else "attention"
        write_health(checks, status, actions)
        return 0 if status == "ok" else 1

    # 2) rebuild (parse -> ... -> QA). The marker survives until the rebuild
    # completes, so a crash mid-rebuild is retried on the next run.
    with open(REBUILD_MARKER, "w", encoding="utf-8") as f:
        f.write(TS)
    rebuild_ok = True
    for script in REBUILD_STEPS:
        if not run_step(script):
            log.warning("%s failed -- attempting corrupt-raw repair pass", script)
            if attempt_repair(actions):
                break
            rebuild_ok = False
            break
    if not rebuild_ok:
        write_health(self_check(), "attention", actions + ["rebuild: FAILED-abort"])
        return 1
    try:
        os.remove(REBUILD_MARKER)
    except OSError:
        pass

    # 3) self-check + auto-rectify (one repair pass max)
    checks = self_check()
    bad = [k for k, v in checks.items() if not v[0]]
    if bad:
        log.warning("Check failures: %s -- attempting repair pass", bad)
        if attempt_repair(actions):
            checks = self_check()
            bad = [k for k, v in checks.items() if not v[0]]
            status = "repaired" if not bad else "attention"
            actions.append("repair pass executed")
        else:
            status = "attention"
            actions.append("no corrupt raw found; needs human look at " + ",".join(bad))
    else:
        status = "ok"
    write_health(checks, status, actions)
    log.info("DONE status=%s -- reports/health.json", status)
    return 0 if status in ("ok", "repaired") else 1


if __name__ == "__main__":
    raise SystemExit(main())
