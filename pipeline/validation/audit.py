"""Hostile audit: try to break the dataset with independent cross-checks.

A. Trading-calendar consistency across 3 independent NSE file families
B. ISIN check-digit validation (mechanical)
C. Exchange's own arithmetic: turnover vs volume*avg_price
D. MTO vs sec_bhavdata_full delivery agreement (2 independent sources)
E. Global cliff elimination: raw vs adjusted >30% jump counts
F. Residual jumps on documented action days
G. Inspect the 8 deliv>volume rows
H. Symbol<->ISIN structure sanity
"""
import glob
import io
import os
import random
import sys
import zipfile

import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
_PROCESSING = os.path.join(_ROOT, "pipeline", "processing")
for _p in [_ROOT, _COMMON, _PROCESSING]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
import parse_standardize as ps

FINAL = os.path.join(_ROOT, "data", "final")
RAW = os.path.join(_ROOT, "data", "raw")
random.seed(42)

print("=" * 72)
print("A. TRADING CALENDAR CROSS-VALIDATION (NSE: cm vs udiff vs MTO vs full)")
dates = {"cm": set(), "mto": set(), "udiff": set(), "full": set()}
MONS = {m: i + 1 for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])}
for f in glob.glob(os.path.join(RAW, "nse/cm/*/cm*")):
    b = os.path.basename(f)  # cm05JAN2015bhav.csv.zip
    dates["cm"].add(pd.Timestamp(int(b[7:11]), MONS[b[4:7]], int(b[2:4])))
for f in glob.glob(os.path.join(RAW, "nse/mto/*/*")):
    b = os.path.basename(f)  # MTO_05012015.DAT
    dates["mto"].add(pd.Timestamp(int(b[8:12]), int(b[6:8]), int(b[4:6])))
for f in glob.glob(os.path.join(RAW, "nse/udiff/*/*")):
    b = os.path.basename(f)  # BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip
    d = b.split("_")[6]
    dates["udiff"].add(pd.Timestamp(int(d[:4]), int(d[4:6]), int(d[6:8])))
for f in glob.glob(os.path.join(RAW, "nse/full/*/*")):
    b = os.path.basename(f)  # sec_bhavdata_full_08082022.csv
    dates["full"].add(pd.Timestamp(int(b[22:26]), int(b[20:22]), int(b[18:20])))

prices = dates["cm"] | dates["udiff"]
print(f"  price-file days: {len(prices)} | MTO days: {len(dates['mto'])} | full days: {len(dates['full'])}")
only_mto = dates["mto"] - prices
print(f"  MTO days with NO price file (trading days we might be missing): {len(only_mto)}")
if only_mto:
    print("   ", sorted(x.date() for x in only_mto)[:15])
full_extra = dates["full"] - prices
print(f"  full-file days with no price file: {len(full_extra)}", sorted(x.date() for x in full_extra)[:10])
mto_full_diff = dates["mto"] - dates["full"]
mto_full_diff = {d for d in mto_full_diff if d >= pd.Timestamp("2019-10-01")}
print(f"  days >=Oct2019 in MTO but not in full: {len(mto_full_diff)}")

print("=" * 72)
print("B. ISIN CHECK-DIGIT VALIDATION")
def isin_valid(u):
    if not isinstance(u, str) or len(u) != 12 or not u[:2].isalpha():
        return None
    s = ""
    for ch in u[:11]:
        s += str(ord(ch) - 55) if ch.isalpha() else ch
    digit, payload = int(u[11]), s[::-1]
    total = 0
    for i, ch in enumerate(payload):
        n = int(ch) * (2 if i % 2 == 0 else 1)
        total += n if n < 10 else n - 9
    return (10 - total % 10) % 10 == digit

df = pd.read_parquet(os.path.join(FINAL, "nse_equity_2015_2026.parquet"),
                     columns=["isin", "symbol", "series", "date", "close", "adj_close",
                              "volume", "turnover", "avg_price", "prev_close"])
isin_nse = pd.Series(df["isin"].unique()).dropna()
bad_nse = [u for u in isin_nse if isin_valid(u) is False]
na_nse = int(df["isin"].isna().sum())
print(f"  NSE ISINs: {isin_nse.nunique()}, invalid check-digit: {len(bad_nse)} {bad_nse[:5]}")
print(f"  NSE rows with missing ISIN: {na_nse}")

print("=" * 72)
print("C. EXCHANGE'S OWN ARITHMETIC: |turnover - volume*avg_price| / turnover")
df["ap"] = pd.to_numeric(df["avg_price"], errors="coerce")
t = pd.to_numeric(df["turnover"], errors="coerce")
v = pd.to_numeric(df["volume"], errors="coerce")
m = (t > 0) & v.notna() & df["ap"].notna()
rel = ((t[m] - v[m] * df["ap"][m]).abs() / t[m])
print(f"  NSE rows checked: {m.sum():,} | within 1%: {(rel < 0.01).mean():.4%} | within 0.1%: {(rel < 0.001).mean():.4%}")

print("=" * 72)
print("D. MTO vs sec_bhavdata_full DELIVERY AGREEMENT (independent sources)")
days = sorted(dates["mto"] & dates["full"])
sample = random.sample(days, 40)
tot, agree, close_enough, checked = 0, 0, 0, 0
for d in sample:
    f_mto = glob.glob(os.path.join(RAW, f"nse/mto/{d.year}/MTO_{d:%d%m%Y}.DAT"))
    f_full = glob.glob(os.path.join(RAW, f"nse/full/{d.year}/sec_bhavdata_full_{d:%d%m%Y}.csv"))
    if not (f_mto and f_full):
        continue
    m1 = ps.mto_deliv(f_mto[0])
    try:
        m2 = ps.parse_nse_full(f_full[0])
    except Exception:
        try:
            m2 = ps.parse_nse_full.__wrapped__ if False else None
            x = pd.read_excel(f_full[0], sheet_name=0)
            # minimal normalize
            x.columns = [c.strip() for c in x.columns]
            for c in x.columns:
                if x[c].dtype == object:
                    x[c] = x[c].astype(str).str.strip()
            m2 = pd.DataFrame({"symbol": x["SYMBOL"], "series": x["SERIES"],
                               "deliv_qty": pd.to_numeric(x["DELIV_QTY"], errors="coerce")})
        except Exception:
            continue
    if m1 is None or m2 is None or not len(m1) or not len(m2):
        continue
    mm = (m1[["symbol", "series", "deliv_qty"]]
          .merge(m2[["symbol", "series", "deliv_qty"]], on=["symbol", "series"],
                 suffixes=("_mto", "_full")))
    mm = mm.dropna()
    big = mm[mm["deliv_qty_mto"] > 100]
    checked += len(big)
    agree += (big["deliv_qty_mto"] == big["deliv_qty_full"]).sum()
    close_enough += (big["deliv_qty_mto"] - big["deliv_qty_full"]).abs().div(
        big["deliv_qty_mto"]).lt(0.01).sum()
    tot += 1
print(f"  days sampled: {tot} | delivery rows compared: {checked:,}")
print(f"  exact match: {agree/checked:.2%} | within 1%: {close_enough/checked:.2%}")

print("=" * 72)
print("E. GLOBAL CLIFF ELIMINATION (>30% 1-day jumps)")
def jump_count(d, col):
    d = d.sort_values(["isin", "date"])
    r = d.groupby("isin", observed=True)[col].pct_change().abs()
    return int((r > 0.30).sum())
df["adj_close_num"] = pd.to_numeric(df["adj_close"], errors="coerce")
df = df.dropna(subset=["date"])
raw_j = jump_count(df, "close")
adj_j = jump_count(df, "adj_close_num")
print(f"  NSE raw jumps >30%: {raw_j:,} -> after adjustment: {adj_j:,}")

print("=" * 72)
print("F. RESIDUAL JUMPS ON DOCUMENTED ACTION DAYS (NSE)")
ca = pd.read_parquet(os.path.join(FINAL, "corporate_actions.parquet"))
cap = ca[(ca["cap_factor"] > 1.001) | (ca["cap_factor"] < 0.999)]
mm = df.merge(cap[["isin", "ex_date"]], on="isin", how="inner")
mm = mm.sort_values(["isin", "date"])
mm["prev_date"] = mm.groupby("isin", observed=True)["date"].shift(1)
mm["prev_adj"] = mm.groupby("isin", observed=True)["adj_close_num"].shift(1)
on_day = mm[(mm["date"] - mm["ex_date"]).dt.days == 0]
jump_mask = (on_day["adj_close_num"] / on_day["prev_adj"] - 1).abs() > 0.30
jump_mask &= (on_day["date"] - on_day["prev_date"]).dt.days <= 7
j = on_day[jump_mask]
print(f"  security-days with a documented capital action: {len(on_day):,}")
print(f"  adjusted series still jumping >30% on those exact days: {len(j)}")
if len(j):
    print(j[["date", "isin", "symbol", "close", "adj_close_num"]].head(10).to_string(index=False))

print("=" * 72)
print("G. ROWS WITH deliv_qty > volume (NSE)")
dv = pd.to_numeric(df["deliv_qty"], errors="coerce") if "deliv_qty" in df.columns else None
if dv is None:
    df2 = pd.read_parquet(os.path.join(FINAL, "nse_equity_2015_2026.parquet"),
                          columns=["date", "symbol", "series", "volume", "deliv_qty"])
    bad = df2[pd.to_numeric(df2["deliv_qty"], errors="coerce") >
              pd.to_numeric(df2["volume"], errors="coerce") + 1]
    print(bad.head(10).to_string(index=False))
    print(f"  total: {len(bad)} | series mix: {bad['series'].value_counts().to_dict()}")

print("=" * 72)
print("H. SYMBOL <-> ISIN STRUCTURE")
sym_multi = df.groupby("symbol", observed=True)["isin"].nunique()
print(f"  NSE symbols mapping to >1 ISIN (reuse over time): {(sym_multi > 1).sum()} of {len(sym_multi):,}")
isin_multi = df.groupby("isin", observed=True)["symbol"].nunique()
print(f"  NSE ISINs with >1 symbol (renames): {(isin_multi > 1).sum()}")
# same-day same-symbol multi-ISIN = real problem; same symbol different eras = fine
d2 = df[["date", "symbol", "isin"]].drop_duplicates()
dd = d2.groupby(["date", "symbol"])["isin"].nunique()
print(f"  same-day same-symbol collisions (must be 0): {(dd > 1).sum()}")
