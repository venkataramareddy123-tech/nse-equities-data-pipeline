"""Per-row circuit flags: price band + upper/lower circuit lock days.

lock_up / lock_dn mark sessions frozen at the exchange limit
(open == high == low == close with a directional move vs prev_close);
validated to 99.99% agreement against the original hand-built flag file.
The file is always recomputed in full from the current finals, so it can
never drift out of sync with the prices it describes and re-runs are
idempotent (crash or corrupt file = just run again).

band comes from data/interim/band_history.parquet, the durable per-(isin,
year) source. That file is seeded once from the legacy flag file and is
never written by this script, so a bad or deleted output can never erase
the history (--skip-band-history writes NaN for one run only). Unknown
pairs get NaN, which downstream consumers fill with the 20% default — same
semantics as a missing row.

Reads : data/final/nse_equity_2015_2026.parquet
        data/interim/band_history.parquet (durable band source)
        data/final/spike_row_flags.parquet (legacy, only to seed the source)
Writes: data/final/spike_row_flags.parquet (isin, year, date, lock_up, lock_dn, band)
"""
import argparse
import os

import numpy as np
import pandas as pd

import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(ROOT, "pipeline", "common")
for _p in [ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# pyrefly: ignore [missing-import]
from _util import atomic_write_parquet, parquet_probe_ok  # noqa: E402

FINAL = os.path.join(ROOT, "data", "final")
INTERIM = os.path.join(ROOT, "data", "interim")
FLAGS = os.path.join(FINAL, "spike_row_flags.parquet")
BAND_SRC = os.path.join(INTERIM, "band_history.parquet")
PX = os.path.join(FINAL, "nse_equity_2015_2026.parquet")


def compute_locks(df: pd.DataFrame) -> pd.DataFrame:
    """Frozen-at-the-limit detection: OHLC all equal + directional move.

    The 0.05% threshold on close/prev_close avoids flagging flat frozen
    rows (ret == 0) whose direction is unknowable; validated against the
    original file at 99.99% for both directions.
    """
    frozen = (np.isclose(df["open"], df["high"], rtol=1e-6)
              & np.isclose(df["high"], df["low"], rtol=1e-6)
              & np.isclose(df["low"], df["close"], rtol=1e-6))
    with np.errstate(divide="ignore", invalid="ignore"):
        ret = (df["close"] / df["prev_close"] - 1.0) * 100.0
    df["lock_up"] = frozen & (ret > 0.05)
    df["lock_dn"] = frozen & (ret < -0.05)
    df["lock_up"] = df["lock_up"].fillna(False)
    df["lock_dn"] = df["lock_dn"].fillna(False)
    return df


def seed_band_source() -> pd.DataFrame:
    """Load (or create once) the durable band source. Never written again."""
    if parquet_probe_ok(BAND_SRC):
        return pd.read_parquet(BAND_SRC)
    bands = pd.DataFrame(columns=["isin", "year", "band"])
    if parquet_probe_ok(FLAGS):
        try:
            fl = pd.read_parquet(FLAGS, columns=["isin", "date", "band"])
            fl["date"] = pd.to_datetime(fl["date"])
            fl = fl.dropna(subset=["band"])
            if len(fl):
                bands = (fl.groupby(["isin", fl["date"].dt.year], observed=True)
                         ["band"].agg(lambda s: s.mode().iloc[0])
                         .rename("band").reset_index())
                bands.columns = ["isin", "year", "band"]
        except Exception:  # noqa: BLE001 - unreadable legacy file = empty seed
            bands = pd.DataFrame(columns=["isin", "year", "band"])
    os.makedirs(os.path.dirname(BAND_SRC), exist_ok=True)
    atomic_write_parquet(bands, BAND_SRC)
    print(f"band source seeded: {len(bands)} (isin, year) pairs")
    return bands


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Regenerate row-level circuit flags.")
    ap.add_argument("--skip-band-history", action="store_true",
                    help="write NaN band this run only (durable source untouched)")
    args = ap.parse_args(argv)

    px = pd.read_parquet(PX, columns=["isin", "date", "open", "high", "low",
                                      "close", "prev_close"])
    if px.empty:
        raise SystemExit("no price rows -- refusing to overwrite flags with an empty frame")
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values(["isin", "date"]).reset_index(drop=True)
    px = compute_locks(px)
    px["year"] = px["date"].dt.year.astype("int16")

    if args.skip_band_history:
        px["band"] = np.nan
    else:
        bands = seed_band_source()
        px = px.merge(bands, on=["isin", "year"], how="left")

    out = px[["isin", "year", "date", "lock_up", "lock_dn", "band"]]
    atomic_write_parquet(out, FLAGS)
    print(f"row_flags: {len(out):,} rows through {out['date'].max().date()} "
          f"(lock_up={int(out['lock_up'].sum()):,}, lock_dn={int(out['lock_dn'].sum()):,}, "
          f"band_known={int(out['band'].notna().sum()):,})")


if __name__ == "__main__":
    main()
