"""Spot-check well-known corporate actions against the final adjusted dataset."""
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
FINAL = os.path.join(_ROOT, "data", "final")


def check(df, isin, around, label):
    d = df[df["isin"] == isin].sort_values("date")
    if not len(d):
        print(f"?? {label}: ISIN {isin} not found")
        return
    a = d[(d["date"] >= pd.Timestamp(around) - pd.Timedelta(days=5))
          & (d["date"] <= pd.Timestamp(around) + pd.Timedelta(days=5))]
    print(f"\n== {label} (around {around})")
    print(a[["date", "close", "prev_close", "adj_cap_factor", "adj_close"]].to_string(index=False))
    if len(a) >= 2:
        r = a["adj_close"].iloc[-1] / a["adj_close"].iloc[0] - 1
        print(f"   adjusted move over window: {r:+.2%} (raw would show the corporate-action cliff)")


def main():
    df = pd.read_parquet(os.path.join(FINAL, "nse_equity_2015_2026.parquet"),
                         columns=["date", "isin", "symbol", "series", "close", "prev_close",
                                  "adj_cap_factor", "adj_close"])
    df = df[df["series"] == "EQ"]

    # 1) Reliance 1:1 bonus, ex-date 2017-09-07 (ISIN INE002A01018)
    check(df, "INE002A01018", "2017-09-07", "Reliance 1:1 bonus (raw close halves, adj continuous)")
    # 2) IRCTC 1:5 split (FV 10->2), ex-date 2021-05-27? ISIN INE347Y01016
    check(df, "INE347Y01016", "2021-05-27", "IRCTC 1:5 split")
    # 3) Infosys 1:1 bonus 2018-09-04? INE009A01021
    check(df, "INE009A01021", "2018-09-04", "Infosys 1:1 bonus 2018")


if __name__ == "__main__":
    main()
