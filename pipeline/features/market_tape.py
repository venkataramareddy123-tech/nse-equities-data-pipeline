"""Market tape state per session (for the tape gate): breadth + drift.

up_frac[date] = fraction of EQ stocks with positive close-to-close return.
mom5[date]    = median 5-session drift across EQ stocks.
Both use only information through date's close (point-in-time for T signals).

Reads : nse_equity_2015_2026.parquet (EQ adj_close)
Writes: data/final/market_tape.parquet (date, up_frac, mom5)
"""
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(ROOT, "pipeline", "common")
for _p in [ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from pipeline.common._util import atomic_write_parquet
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_parquet


FINAL = os.path.join(ROOT, "data", "final")


def main():
    px = pd.read_parquet(os.path.join(FINAL, "nse_equity_2015_2026.parquet"),
                         columns=["isin", "date", "adj_close"])
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values(["isin", "date"])
    g = px.groupby("isin", observed=True)["adj_close"]
    px["ret1"] = g.pct_change()
    px["ret5"] = g.pct_change(5)
    tape = px.groupby("date").agg(up_frac=("ret1", lambda s: float((s > 0).mean())),
                                  mom5=("ret5", "median")).reset_index()
    atomic_write_parquet(tape, os.path.join(FINAL, "market_tape.parquet"))
    print(f"tape sessions: {len(tape)}")
    print(tape["up_frac"].describe().round(3).to_string())


if __name__ == "__main__":
    main()
