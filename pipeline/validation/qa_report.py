"""Generate the data quality report + machine-readable QA summary.

Reads final parquets and produces reports/data_quality_report.md
"""
import glob
import json
import os
import sys
from collections import defaultdict

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from pipeline.common._util import atomic_write_csv
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_csv


BASE = os.path.join(_ROOT, "data")
FINAL = os.path.join(BASE, "final")
REPORT = os.path.join(_ROOT, "reports")

S = defaultdict(dict)


def ohlc_checks(df, label):
    o, h, l, c = (df[x].astype(float) for x in ("open", "high", "low", "close"))
    checks = {
        "close<=0": int((c <= 0).sum()),
        "close NaN": int(c.isna().sum()),
        "high<low": int((h < l).sum()),
        "close>high": int((c > h + 0.01).sum()),
        "close<low": int((c < l - 0.01).sum()),
        "open>high": int((o > h + 0.01).sum()),
        "open<low": int((o < l - 0.01).sum()),
        "volume<0": int((df["volume"].astype(float) < 0).sum()),
        "volume==0 rows": int((df["volume"].astype(float) == 0).sum()),
    }
    S[label]["ohlc"] = checks
    return checks


def delivery_checks(df, label):
    d = df["deliv_pct"].astype(float)
    dv = df["deliv_qty"].astype(float)
    vol = df["volume"].astype(float)
    checks = {
        "deliv_pct coverage": float(d.notna().mean()),
        "deliv_pct outside [0,100]": int(((d < 0) | (d > 100)).sum()),
        "deliv_qty > volume": int((dv > vol + 1).sum()),
    }
    S[label]["delivery"] = checks


def continuity_checks(df, label):
    df = df.sort_values("date")
    days = pd.Series(pd.to_datetime(sorted(df["date"].unique())))
    gaps = days.diff().dt.days
    S[label]["calendar"] = {
        "first": str(days.iloc[0].date()), "last": str(days.iloc[-1].date()),
        "n_sessions": int(len(days)),
        "max_gap_days": int(gaps.max()),
        "gaps_over_4_days": int((gaps > 4).sum()),
    }


def prev_close_mismatch(df, label, tol=0.011):
    df = df.sort_values(["isin", "series", "date"])
    g = df.groupby(["isin", "series"], observed=True)
    prev_close_raw = g["close"].shift(1)
    prev_date = g["date"].shift(1)
    pc = df["prev_close"].astype(float)
    recent = (df["date"] - prev_date).dt.days <= 7  # skip resumptions after suspensions
    m = recent & prev_close_raw.notna() & pc.notna()
    bad = (pc[m] - prev_close_raw[m]).abs() / prev_close_raw[m] > tol
    n = int(bad.sum())
    S[label]["prev_close_mismatch_gt_1pct"] = n
    S[label]["prev_close_pairs_checked"] = int(m.sum())
    if n:
        sub = df[m][bad]
        atomic_write_csv(
            sub[["date", "isin", "symbol", "series", "prev_close", "close"]].head(500),
            os.path.join(REPORT, f"prev_close_mismatch_{label}.csv"))
    return n





def yearly_summary(df, label):
    rows = []
    for y, g in df.groupby(df["date"].dt.year):
        rows.append({
            "year": int(y), "sessions": int(g["date"].nunique()),
            "securities": int(g.groupby(["isin", "series"], observed=True).ngroups),
            "unique_isins": int(g["isin"].nunique()),
            "rows": int(len(g)),
            "turnover_cr": float(g["turnover"].astype(float).sum() / 1e7),
        })
    S[label]["yearly"] = rows


def main():
    os.makedirs(REPORT, exist_ok=True)
    for label, fname in [("NSE", "nse_equity_2015_2026.parquet")]:
        fp = os.path.join(FINAL, fname)
        df = pd.read_parquet(fp)
        df["date"] = pd.to_datetime(df["date"])
        ohlc_checks(df, label)
        delivery_checks(df, label)
        continuity_checks(df, label)
        prev_close_mismatch(df, label)
        yearly_summary(df, label)
        S[label]["dupes"] = int(df.duplicated(subset=["date", "symbol", "series"]).sum())

        # security master stats
        S[label]["securities_total"] = int(df["isin"].nunique())
        S[label]["symbols_total"] = int(df["symbol"].nunique())

    # corporate action reconciliation
    rec_fp = os.path.join(FINAL, "corporate_actions_reconciliation.parquet")
    if os.path.exists(rec_fp):
        rec = pd.read_parquet(rec_fp)
        cap = rec[rec["cap_c"] != 1.0]
        S["corp_actions"] = {
            "capital actions reconciled": int(len(cap)),
            "within 12% tolerance": int(cap["recon_ok"].sum()) if len(cap) else 0,
            "gap-estimated factors": int(rec["gap_estimated"].sum()),
            "reorg-flagged": int(rec["reorg"].sum()),
            "fail_examples": cap[~cap["recon_ok"]][["isin", "ex_date", "purpose", "cap_c",
                                                    "expected_ratio", "observed_ratio"]]
                              .head(25).to_dict("records") if len(cap) else [],
        }

    # write summary json + md
    try:
        from pipeline.common._util import atomic_write_json
    except ImportError:
        # pyrefly: ignore [missing-import]
        from _util import atomic_write_json
    atomic_write_json(S, os.path.join(REPORT, "qa_summary.json"))

    md = ["# Data Quality Report", f"_Generated {pd.Timestamp.now():%Y-%m-%d %H:%M}_", ""]
    for label in ["NSE"]:
        if label not in S:
            continue
        md.append(f"## {label}")
        md.append(f"- securities (ISINs): **{S[label]['securities_total']:,}**, "
                  f"symbols: {S[label]['symbols_total']:,}, duplicate rows: {S[label]['dupes']}")
        cal = S[label]["calendar"]
        md.append(f"- calendar: {cal['first']} -> {cal['last']}, {cal['n_sessions']} sessions, "
                  f"max gap {cal['max_gap_days']} days, gaps>4d: {cal['gaps_over_4_days']}")
        md.append(f"- prev_close mismatches >1%: {S[label]['prev_close_mismatch_gt_1pct']:,}")
        md.append("")
        md.append("| check | value |")
        md.append("|---|---|")
        for k, v in S[label]["ohlc"].items():
            md.append(f"| {k} | {v:,} |")
        for k, v in S[label]["delivery"].items():
            md.append(f"| {k} | {v} |")
        md.append("")
        md.append("| year | sessions | securities (ISIN) | rows | turnover (cr) |")
        md.append("|---|---|---|---|---|")
        for r in S[label]["yearly"]:
            md.append(f"| {r['year']} | {r['sessions']} | {r['unique_isins']:,} | "
                      f"{r['rows']:,} | {r['turnover_cr']:,.0f} |")
        md.append("")
    if "corp_actions" in S:
        md.append("## Corporate action reconciliation")
        for k, v in S["corp_actions"].items():
            if k != "fail_examples":
                md.append(f"- {k}: {v}")
        md.append("")
    _md = os.path.join(REPORT, "data_quality_report.md")
    with open(_md + ".part", "w", encoding="utf-8") as f:
        f.write(chr(10).join(md))
    os.replace(_md + ".part", _md)
    print("\n".join(md[:40]))
    print("...")
    print("reports written")


if __name__ == "__main__":
    main()
