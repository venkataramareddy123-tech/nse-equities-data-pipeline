"""Build security master: ISIN-keyed identity with full symbol/name history for NSE.
Outputs: data/final/security_master.parquet (and security_master_nse.parquet)
"""
import glob
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from pipeline.common._util import atomic_write_csv, atomic_write_parquet
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_csv, atomic_write_parquet

BASE = os.path.join(_ROOT, "data")
FINAL = os.path.join(BASE, "final")


def load_exchange(ex="nse"):
    files = sorted(glob.glob(os.path.join(BASE, "interim", ex, "*.parquet")))
    df = pd.concat([pd.read_parquet(f, columns=["date", "symbol", "series", "isin", "name"]) for f in files],
                   ignore_index=True)
    return df


def build_for(df, key_col="isin", ex_label="NSE"):
    df = df.dropna(subset=[key_col])
    df["date"] = pd.to_datetime(df["date"])
    seg = (df.groupby([key_col, "symbol"], as_index=False)
             .agg(first_date=("date", "min"), last_date=("date", "max")))
    names = (df.dropna(subset=["name"])
               .sort_values("date")
               .groupby(key_col, as_index=False)["name"].last()
               .rename(columns={"name": "latest_name"}))
    isins = (df.sort_values("date")
               .groupby(key_col, as_index=False)["isin"].last())
    seg = seg.merge(names, on=key_col, how="left").merge(isins, on=key_col, how="left")
    seg["exchange"] = ex_label
    return seg


def main():
    os.makedirs(FINAL, exist_ok=True)

    nse = load_exchange("nse")
    nse_m = build_for(nse, "isin", "NSE")
    nse_m = nse_m.rename(columns={"symbol": "symbol", "first_date": "seg_start", "last_date": "seg_end"})

    # Save primary security master
    atomic_write_parquet(nse_m, os.path.join(FINAL, "security_master.parquet"))
    atomic_write_parquet(nse_m, os.path.join(FINAL, "security_master_nse.parquet"))

    # Symbol changes report: ISINs with >1 symbol segment
    multi = nse_m.groupby("isin").filter(lambda g: g["symbol"].nunique() > 1)
    multi = multi.sort_values(["isin", "seg_start"])
    atomic_write_csv(multi, os.path.join(FINAL, "symbol_changes.csv"))
    atomic_write_csv(multi, os.path.join(FINAL, "symbol_changes_nse.csv"))
    print(f"NSE: {nse_m['isin'].nunique()} securities, {len(nse_m)} symbol segments, "
          f"{multi['isin'].nunique()} with symbol changes")
if __name__ == "__main__":
    main()
