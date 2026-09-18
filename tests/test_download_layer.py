"""Guardrails for the download/fetch layer.

Pins the failure modes the hostile review found: bot-blocks recorded as
holidays (403->miss), poisoned CA caches trusted forever, HTML error pages
persisted as data, and non-atomic manifest/CA writes.
"""
import json
import os

import pandas as pd

import dl
import fetch_nse_ca
import roll_update


def _touch(path, content=b"x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)


# --- status classification: a block is not a holiday ---------------------------

def test_403_is_fail_not_miss(monkeypatch, tmp_path):
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)

    class R:
        status_code = 403
        content = b""

    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: R())
    st, _ = dl.download_one("u", str(tmp_path / "x.zip"), {}, expect_magic=b"PK")
    assert st == "fail"


def test_404_is_miss(monkeypatch, tmp_path):
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)

    class R:
        status_code = 404
        content = b""

    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: R())
    st, _ = dl.download_one("u", str(tmp_path / "x.zip"), {}, expect_magic=b"PK")
    assert st == "miss"


def test_html_error_page_never_persisted(monkeypatch, tmp_path):
    class R:
        status_code = 200
        content = b"<html>blocked</html>" * 100

    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: R())
    p = tmp_path / "x.zip"
    st, _ = dl.download_one("u", str(p), {}, expect_magic=b"PK")
    assert st == "miss"
    assert not p.exists() and not list(tmp_path.glob("*.part"))


def test_success_is_atomic(monkeypatch, tmp_path):
    class R:
        status_code = 200
        content = b"PK\x03\x04" + b"0" * 2000

    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: R())
    p = tmp_path / "x.zip"
    st, size = dl.download_one("u", str(p), {}, expect_magic=b"PK")
    assert st == "ok" and p.exists() and size == len(R.content)
    assert not list(tmp_path.glob("*.part"))


def test_manifest_survives_crash(monkeypatch, tmp_path):
    # run_jobs writes the manifest atomically; simulate by checking the
    # manifest path never exists as a 0-byte file after a normal run.
    jobs = []
    mp = tmp_path / "manifest.csv"
    dl.run_jobs(jobs, 1, str(mp))
    assert mp.exists() and mp.stat().st_size > 0
    assert not list(tmp_path.glob("*.part"))


# --- NSE CA cache: only parseable JSON lists are trusted -----------------------

def _ca(path, payload, raw=None):
    with open(path, "w", encoding="utf-8") as f:
        f.write(raw if raw is not None else json.dumps(payload))


def test_ca_cache_valid_list_ok(tmp_path):
    p = tmp_path / "ca.json"
    _ca(p, [{"sym": "X"}] * 10)
    assert fetch_nse_ca.cached_ok(str(p)) is True


def test_ca_cache_rejects_error_object(tmp_path):
    p = tmp_path / "ca.json"
    _ca(p, {"error": "blocked"})  # NSE soft-block: HTTP 200 + JSON object
    assert fetch_nse_ca.cached_ok(str(p)) is False


def test_ca_cache_rejects_truncated(tmp_path):
    p = tmp_path / "ca.json"
    _ca(p, None, raw='[{"sym": "A"}, {"sym": "B"')  # killed mid-write
    assert fetch_nse_ca.cached_ok(str(p)) is False


def test_ca_cache_rejects_small_or_missing(tmp_path):
    p = tmp_path / "ca.json"
    _ca(p, [])
    p.write_text("[]")  # <=100 bytes
    assert fetch_nse_ca.cached_ok(str(p)) is False
    assert fetch_nse_ca.cached_ok(str(tmp_path / "nope.json")) is False


# --- corruption scan covers non-zip sources ------------------------------------

def test_find_corrupt_raw_flags_poisoned_nonzip(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    good = ("date,symbol,series,deliv_qty\n" + "2026-01-01,X,EQ,100\n" * 60).encode()
    _touch(raw / "nse" / "mto" / "2026" / "MTO_01012026.DAT", good)
    _touch(raw / "nse" / "mto" / "2026" / "MTO_02012026.DAT",
           b"<html>challenge page</html>" * 40)
    _touch(raw / "nse" / "full" / "2026" / "sec_bhavdata_full_02012026.csv",
           b'{"error": "denied"}' * 50)
    monkeypatch.setattr(roll_update, "BASE", str(tmp_path))
    bad = [os.path.basename(p) for p in roll_update.find_corrupt_raw()]
    assert "MTO_02012026.DAT" in bad
    assert any("sec_bhavdata_full" in b for b in bad)
    assert "MTO_01012026.DAT" not in bad


def test_find_corrupt_raw_flags_torn_zip(tmp_path, monkeypatch):
    import zipfile as zf
    raw = tmp_path / "raw"
    zpath = raw / "nse" / "cm" / "2026" / "cm01022026bhav.csv.zip"
    buf = io_bytes_zip()
    _touch(zpath, buf)
    with open(zpath, "r+b") as f:  # tear the central directory
        f.truncate(len(buf) - 30)
    monkeypatch.setattr(roll_update, "BASE", str(tmp_path))
    assert any("cm01022026" in p for p in roll_update.find_corrupt_raw())


def io_bytes_zip():
    import io
    buf = io.BytesIO()
    with zf_write(buf) as z:
        z.writestr("inner.csv", "a,b\n1,2\n")
    return buf.getvalue()


def zf_write(buf):
    import zipfile as zf
    return zf.ZipFile(buf, "w", zf.ZIP_DEFLATED)


def test_circuit_breaker_aborts_early(monkeypatch, tmp_path):
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)

    class R:
        status_code = 403
        content = b""

    call_count = [0]

    def mock_get(*a, **k):
        call_count[0] += 1
        return R()

    monkeypatch.setattr(dl.requests, "get", mock_get)

    jobs = [("http://example.com", str(tmp_path / f"x_{i}.zip"), {}, b"PK") for i in range(50)]
    mp = tmp_path / "manifest.csv"
    res = dl.run_jobs(jobs, workers=2, manifest_path=str(mp), max_consecutive_fails=5)

    assert len(res) < len(jobs)
    assert call_count[0] < 50 * 3
    assert mp.exists()
    df = pd.read_csv(mp)
    assert len(df) == len(res)


def test_manifest_incremental_flush(monkeypatch, tmp_path):
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)

    class R:
        status_code = 200
        content = b"PK\x03\x04" + b"0" * 2000

    monkeypatch.setattr(dl.requests, "get", lambda *a, **k: R())

    jobs = [("http://example.com", str(tmp_path / f"x_{i}.zip"), {}, b"PK") for i in range(5)]
    mp = tmp_path / "manifest.csv"
    res = dl.run_jobs(jobs, workers=1, manifest_path=str(mp))

    assert len(res) == 5
    df = pd.read_csv(mp)
    assert len(df) == 5
    assert set(df["status"]) == {"ok"}
