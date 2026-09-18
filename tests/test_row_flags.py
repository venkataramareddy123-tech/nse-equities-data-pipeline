"""Guardrails for row_flags: the durable band source must be unfakeable.

Pins the hostile-review findings: the output file was once its own band
history source (one bad run could erase it forever), and --skip-band-history
used to bake the erasure in.
"""
import os

import numpy as np
import pandas as pd

import row_flags as rf


def _mk_parquet(path, df):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def _seed(tmp_path, monkeypatch, flags_df=None, px_df=None):
    final = tmp_path / "final"
    interim = tmp_path / "interim"
    monkeypatch.setattr(rf, "FINAL", str(final))
    monkeypatch.setattr(rf, "INTERIM", str(interim))
    monkeypatch.setattr(rf, "FLAGS", str(final / "spike_row_flags.parquet"))
    monkeypatch.setattr(rf, "BAND_SRC", str(interim / "band_history.parquet"))
    monkeypatch.setattr(rf, "PX", str(final / "px.parquet"))
    if flags_df is not None:
        _mk_parquet(str(final / "spike_row_flags.parquet"), flags_df)
    if px_df is not None:
        _mk_parquet(str(final / "px.parquet"), px_df)
    return final, interim


def _px():
    return pd.DataFrame({
        "isin": ["A", "A", "A", "B"],
        "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-05",
                                "2026-01-01"]),
        "open": [10.0, 10.0, 10.0, 5.0],
        "high": [10.2, 10.2, 10.2, 5.0],
        "low": [9.8, 9.8, 9.8, 5.0],
        "close": [10.0, 10.0, 10.0, 5.0],
        "prev_close": [10.0, 10.0, 10.0, np.nan],
    })


def test_band_source_seeded_once_then_immutable(tmp_path, monkeypatch):
    legacy = pd.DataFrame({
        "isin": ["A", "A"], "date": pd.to_datetime(["2026-01-01", "2026-06-01"]),
        "band": [5.0, 5.0]})  # (A, 2026) -> all three 2026 A rows inherit 5.0
    final, interim = _seed(tmp_path, monkeypatch, flags_df=legacy, px_df=_px())
    rf.main([])
    src = pd.read_parquet(str(interim / "band_history.parquet"))
    assert len(src) == 1 and src.iloc[0]["band"] == 5.0  # (A, 2025)

    # deleting/corrupting the OUTPUT must not destroy the band source
    os.remove(str(final / "spike_row_flags.parquet"))
    rf.main([])
    src2 = pd.read_parquet(str(interim / "band_history.parquet"))
    assert len(src2) == 1 and src2.iloc[0]["band"] == 5.0
    out = pd.read_parquet(str(final / "spike_row_flags.parquet"))
    assert out["band"].notna().sum() == 3  # A rows carry history; B has none


def test_skip_band_history_keeps_source(tmp_path, monkeypatch):
    legacy = pd.DataFrame({
        "isin": ["A"], "date": pd.to_datetime(["2026-01-01"]), "band": [5.0]})
    final, interim = _seed(tmp_path, monkeypatch, flags_df=legacy, px_df=_px())
    rf.main([])                       # first run seeds the band source
    rf.main(["--skip-band-history"])
    out = pd.read_parquet(str(final / "spike_row_flags.parquet"))
    assert out["band"].isna().all()          # this run: no bands
    src = pd.read_parquet(str(interim / "band_history.parquet"))
    assert len(src) == 1                      # durable source untouched
    # next normal run restores bands from the source
    rf.main([])
    out2 = pd.read_parquet(str(final / "spike_row_flags.parquet"))
    assert out2["band"].notna().sum() == 3


def test_lock_flags_frozen_bar(tmp_path, monkeypatch):
    px = pd.DataFrame({
        "isin": ["C"] * 4,
        "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-05",
                                "2026-01-06"]),
        "open": [10.0, 11.0, 9.0, 10.0],
        "high": [10.5, 11.0, 9.0, 10.2],
        "low": [9.5, 11.0, 9.0, 9.8],
        "close": [10.0, 11.0, 9.0, 10.0],  # day3 fully frozen at 9.0
        "prev_close": [10.0, 10.0, 11.0, 10.0],
    })
    _seed(tmp_path, monkeypatch, flags_df=None, px_df=px)
    rf.main([])
    out = pd.read_parquet(str(tmp_path / "final" / "spike_row_flags.parquet"))
    out = out.sort_values("date")
    # day2: frozen at +10% (upper lock); day3: frozen at -18% (lower lock);
    # day1: normal bar; day4: normal bar
    assert list(out["lock_up"]) == [False, True, False, False]
    assert list(out["lock_dn"]) == [False, False, True, False]


def test_empty_px_refuses_overwrite(tmp_path, monkeypatch):
    final, interim = _seed(tmp_path, monkeypatch, flags_df=None,
                           px_df=_px().iloc[:0])
    try:
        rf.main()
        raised = False
    except SystemExit:
        raised = True
    assert raised
    assert not (final / "spike_row_flags.parquet").exists()
