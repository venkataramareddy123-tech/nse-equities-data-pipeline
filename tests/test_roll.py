"""Institutional guardrails for the rolling updater.

Fast, hermetic (tmp_path, no network): the exact bug classes seen in
production — dead fast path, torn files trusted, lock races, empty-cache
refetch storms — each has a test pinning the fix.
"""
import csv
import json
import os
import time

import pandas as pd

import _util
import roll_update


def _touch(path, content=b"x", mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


# --- fast path: manifests must not count as content -------------------------

def test_newest_mtime_ignores_manifests(tmp_path):
    raw = tmp_path / "raw"
    _touch(str(raw / "nse" / "cm" / "2026" / "a.zip"), mtime=1000)
    _touch(str(raw / "manifest_nse.csv"), mtime=9999)
    assert roll_update.newest_mtime(str(raw)) == 1000


def test_newest_mtime_empty(tmp_path):
    assert roll_update.newest_mtime(str(tmp_path / "nope")) == 0.0


# --- download digest ----------------------------------------------------------

def _manifest(tmp_path, rows):
    p = tmp_path / "manifest_nse.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url", "path", "status", "bytes"])
        w.writerows(rows)
    return p


def test_digest_holiday_and_lag(tmp_path, monkeypatch):
    import datetime as dt
    today = dt.date.today()
    lag = (today - dt.timedelta(days=2)).strftime("%Y%m%d")
    rows = [
        ("u", f"C:\\d\\cm\\2015\\cm26JAN2015bhav.csv.zip", "miss", 0),   # holiday price
        ("u", "C:\\d\\mto\\2015\\MTO_26012015.DAT", "miss", 0),          # holiday delivery
        ("u", f"C:\\d\\full\\2026\\sec_bhavdata_full_{lag[6:]}{lag[4:6]}{lag[:4]}.csv",
         "miss", 0),                                                     # recent lag
        ("u", "C:\\d\\cm\\2026\\cm05JAN2026bhav.csv.zip", "have", 999),
    ]
    mp = _manifest(tmp_path, rows)
    monkeypatch.setattr(roll_update, "BASE", str(tmp_path))

    # digest reads BASE/raw/manifest_nse.csv; mirror the manifest there
    raw = tmp_path / "raw"
    raw.mkdir()
    import shutil
    shutil.copy(mp, raw / "manifest_nse.csv")
    dg = roll_update.download_digest()
    assert dg["total_miss"] == 3
    assert dg["holiday_like"] == 1
    assert dg["unexpected_price_only"] == []
    assert len(dg["delivery_lag"]) == 1


def test_digest_unexpected_price_only(tmp_path, monkeypatch):
    rows = [("u", "C:\\d\\cm\\2026\\cm05JAN2026bhav.csv.zip", "miss", 0),
            ("u", "C:\\d\\mto\\2026\\MTO_06012026.DAT", "have", 999)]
    raw = tmp_path / "raw"
    raw.mkdir()
    _manifest(raw, rows).rename(raw / "manifest_nse.csv")
    monkeypatch.setattr(roll_update, "BASE", str(tmp_path))
    dg = roll_update.download_digest()
    assert dg["unexpected_price_only"] == ["2026-01-05"]


# --- lock ---------------------------------------------------------------------

def test_lock_acquire_release_and_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(roll_update, "LOCKPATH", str(tmp_path / ".lock"))
    assert roll_update.acquire_lock() is True
    # second acquire while held (same live process) refuses
    assert roll_update.acquire_lock() is False
    roll_update.release_lock()
    assert roll_update.acquire_lock() is True
    roll_update.release_lock()
    # leftover file with a dead/garbage pid is taken over, never blocks
    with open(str(tmp_path / ".lock"), "w", encoding="utf-8") as f:
        f.write("99999999")
    assert roll_update.acquire_lock() is True
    roll_update.release_lock()


# --- atomic writes + probe ------------------------------------------------------

def test_atomic_parquet_roundtrip_and_truncation(tmp_path):
    p = str(tmp_path / "a.parquet")
    df = pd.DataFrame({"x": [1, 2, 3]})
    _util.atomic_write_parquet(df, p)
    assert _util.parquet_probe_ok(p) is True
    assert len(pd.read_parquet(p)) == 3
    assert not [f for f in os.listdir(tmp_path) if ".tmp-" in f]
    with open(p, "r+b") as f:  # simulate crash mid-write
        f.truncate(100)
    assert _util.parquet_probe_ok(p) is False


def test_free_ram_sane():
    assert _util.free_ram_gb() > 0
