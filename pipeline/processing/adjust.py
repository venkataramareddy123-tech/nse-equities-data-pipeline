"""Apply corporate-action adjustments + reconcile against raw ex-date gaps.

Outputs:
  data/final/nse_equity_2015_2026.parquet  (raw + adjusted OHLC + factors)
  data/final/corporate_actions.parquet
  reports: reconciliation + undocumented gap anomalies
"""
import glob
import os
import sys

# pyrefly: ignore [missing-import]
import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# pyrefly: ignore [missing-import]
from _util import atomic_write_csv, atomic_write_parquet

BASE = os.path.join(_ROOT, "data")
FINAL = os.path.join(BASE, "final")
REPORT = os.path.join(_ROOT, "reports")


def load_all(ex):
    files = sorted(glob.glob(os.path.join(BASE, "interim", ex, "*.parquet")))
    cols = ["date", "exchange", "symbol", "series", "isin", "name", "open", "high",
            "low", "close", "prev_close", "last", "avg_price", "volume", "turnover",
            "trades", "deliv_qty", "deliv_pct"]
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    for c in ["symbol", "series", "isin", "exchange", "name"]:
        if c in df.columns:
            df[c] = df[c].astype("category")
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols + [c for c in df.columns if c not in cols]].copy()


def consolidate_actions(ca):
    """One merged NSE action per (isin, ex_date)."""
    ca = ca[ca["isin"].notna() & (ca["isin"] != "")]
    out = {}
    conflicts = []
    for r in ca.itertuples():
        if pd.isna(r.ex_date):
            continue
        k = (r.isin, r.ex_date)
        a = out.setdefault(k, {
            "cap_c": 1.0, "div": 0.0, "rights": [], "reorg": False,
            "unresolved_cap": False, "div_unresolved": False,
            "sources": set(), "purpose_raw": [],
            "fv": None, "bonus_done": False, "split_done": False,
            "rights_done": False, "div_done": False,
        })
        a["sources"].add(r.source)
        if isinstance(r.purpose_raw, str):
            a["purpose_raw"].append(r.purpose_raw)

        t = str(r.type)
        has_cap = any(x in t for x in ("bonus", "split", "consolidation", "rights"))
        if "reorg" in t:
            a["reorg"] = True

        # same component from both feeds = same event: apply once
        if "bonus" in t and not a["bonus_done"]:
            if pd.notna(r.a_num) and pd.notna(r.a_den) and r.a_den:
                a["cap_c"] *= (r.a_num + r.a_den) / r.a_den
                a["bonus_done"] = True
            else:
                a["unresolved_cap"] = True
        if ("split" in t or "consolidation" in t) and not a["split_done"]:
            if pd.notna(r.fv_old) and pd.notna(r.fv_new) and r.fv_new:
                a["cap_c"] *= r.fv_old / r.fv_new
                a["fv"] = (r.fv_old, r.fv_new)
                a["split_done"] = True
            else:
                a["unresolved_cap"] = True
        if "rights" in t and not a["rights_done"]:
            if pd.notna(r.a_num) and pd.notna(r.a_den) and r.a_den:
                a["rights"].append((r.a_num, r.a_den, r.rights_price))
                a["rights_done"] = True
            else:
                a["unresolved_cap"] = True
        if has_cap and not (a["bonus_done"] or a["split_done"] or a["rights_done"]):
            a["unresolved_cap"] = True
        # Multiple NSE notices can describe the same dividend.
        if "dividend" in t:
            amt = r.div_amount
            if pd.isna(amt) and pd.notna(r.div_pct) and pd.notna(r.fv):
                amt = r.div_pct / 100.0 * r.fv
            if amt is not None and not pd.isna(amt):
                if not a["div_done"]:
                    a["div"] = float(amt)
                    a["div_done"] = True
                elif r.source == "NSE":
                    if abs(a["div"] - amt) / max(a["div"], 1e-9) > 0.02:
                        conflicts.append((r.isin, r.ex_date, a["div"], amt))
                    a["div"] = float(amt)
            elif not a["div_done"]:
                # Percentage-of-face-value dividend with unknown face value:
                # unapplied, but tracked so it cannot rot silently.
                a["div_unresolved"] = True
    return out, conflicts


def apply_adjustments(df, actions_by_isin, key_col="isin"):
    """df: one exchange's price rows. Returns df with factor/adjusted columns."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values([key_col, "date"])
    cap_f = np.ones(len(df))
    tr_f = np.ones(len(df))
    gap_est = np.zeros(len(df), dtype=bool)

    df_dates = df["date"].values.astype("datetime64[ns]").astype("int64")
    idx_groups = df.groupby(key_col, sort=False).indices

    recon_rows = []
    for key, idx in idx_groups.items():
        acts = actions_by_isin.get(key)
        idx = np.asarray(idx)
        if acts is None or len(idx) < 2:
            continue
        dates = df_dates[idx]
        closes = df["close"].values[idx].astype(float)
        opens = df["open"].values[idx].astype(float)
        caps = np.ones(len(idx))
        trs = np.ones(len(idx))
        est = np.zeros(len(idx), dtype=bool)

        for ex_date, a in sorted(acts.items(), key=lambda kv: kv[0]):
            ex_ns = pd.Timestamp(ex_date).value  # ns since epoch, matches df_dates
            pos = np.searchsorted(dates, ex_ns)
            if pos == 0 or pos >= len(dates):
                continue  # action outside price coverage
            # price on last session strictly before ex-date
            j = pos - 1
            p_prev = closes[j]
            if not np.isfinite(p_prev) or p_prev <= 0:
                continue
            c = a["cap_c"]
            d = a["div"]
            thelp = None
            if a["rights"]:
                # sequential rights (rare); apply each: THEP formula
                p = p_prev
                for (n, dd, rp) in a["rights"]:
                    if rp is None or pd.isna(rp) or rp <= 0:
                        continue
                    thelp = (dd * p + n * rp) / (dd + n)
                    p = thelp
                if thelp and thelp > 0:
                    c *= p_prev / thelp
            gap_est_flag = False
            if a["rights"] and thelp is None:
                # Rights ratio known but premium unresolvable: estimate the
                # factor from the observed gap and flag it instead of
                # silently leaving the series discontinuous.
                px = opens[pos] if np.isfinite(opens[pos]) and opens[pos] > 0 else closes[pos]
                if np.isfinite(px) and px > 0:
                    est_c = p_prev / px
                    if 0.15 <= est_c <= 6.67:
                        c *= est_c
                        gap_est_flag = True
            if a["unresolved_cap"] and c == 1.0 and not a["rights"] and not a["reorg"]:
                # documented capital action, ratio unknown -> estimate from gap
                px = opens[pos] if np.isfinite(opens[pos]) and opens[pos] > 0 else closes[pos]
                if np.isfinite(px) and px > 0:
                    est_c = p_prev / px
                    if 0.15 <= est_c <= 6.67:
                        c = est_c
                        gap_est_flag = True
            if c == 1.0 and d == 0.0 and not a["reorg"]:
                continue
            if gap_est_flag:
                est[:pos] = True  # group-local: rows before the ex-date
            f = c * (1.0 + (d / p_prev if p_prev > 0 else 0.0))
            caps[:pos] *= c
            trs[:pos] *= f
            # reconcile: observed vs expected ex-date move
            p_ex = closes[pos] if np.isfinite(closes[pos]) else np.nan
            if np.isfinite(p_ex) and p_ex > 0:
                exp_ratio = 1.0 / c
                obs_ratio = p_ex / p_prev
                ok = (abs(obs_ratio - exp_ratio) / exp_ratio) < 0.12
                recon_rows.append({
                    "isin": key, "ex_date": pd.Timestamp(ex_date), "sources": "|".join(sorted(a["sources"])),
                    "purpose": " || ".join(a["purpose_raw"])[:180],
                    "cap_c": c, "div": d, "prev_close": p_prev, "ex_close": p_ex,
                    "expected_ratio": exp_ratio, "observed_ratio": obs_ratio,
                    "recon_ok": bool(ok), "gap_estimated": gap_est_flag,
                    "reorg": a["reorg"],
                })
        cap_f[idx] = caps
        tr_f[idx] = trs
        gap_est[idx] = est

    df["adj_cap_factor"] = cap_f        # cumulative capital multiplier — DIVIDE raw OHLC by it
    df["adj_tr_factor"] = tr_f          # cumulative total-return multiplier — DIVIDE close by it
    df["factor_gap_estimated"] = gap_est
    for c in ["open", "high", "low", "close"]:
        df[f"adj_{c}"] = df[c].astype(float) / df["adj_cap_factor"]
    df["adj_close_tr"] = df["close"].astype(float) / df["adj_tr_factor"]
    recon = pd.DataFrame(recon_rows)
    return df, recon


def undocumented_gaps(df, key_col="isin", thresh=0.25):
    df = df.sort_values([key_col, "date"])
    g = df.groupby(key_col)["adj_close"]
    prev = g.shift(1)
    ratio = df["adj_close"] / prev
    m = (ratio.notna()) & ((ratio - 1).abs() > thresh)
    return df[m].copy()


def main():
    os.makedirs(FINAL, exist_ok=True)
    os.makedirs(REPORT, exist_ok=True)

    ca = pd.read_parquet(os.path.join(BASE, "interim", "corporate_actions.parquet"))
    actions, conflicts = consolidate_actions(ca)
    actions_by_isin = {}
    for (isin, exd), a in actions.items():
        actions_by_isin.setdefault(isin, {})[exd] = a
    print(f"actions consolidated: {len(actions)} (isin,ex_date) pairs; div conflicts: {len(conflicts)}")

    # save consolidated action table
    rows = []
    for (isin, exd), a in actions.items():
        rows.append({
            "isin": isin, "ex_date": pd.Timestamp(exd),
            "cap_factor": a["cap_c"], "div_amount": a["div"],
            "rights": str(a["rights"]), "reorg": a["reorg"],
            "unresolved_capital": a["unresolved_cap"],
            "div_unresolved": a["div_unresolved"],
            "sources": "|".join(sorted(a["sources"])),
            "purpose_raw": " || ".join(a["purpose_raw"])[:300],
        })
    atomic_write_parquet(
        pd.DataFrame(rows).sort_values(["isin", "ex_date"]),
        os.path.join(FINAL, "corporate_actions.parquet"))

    # Process NSE adjustments
    df = load_all("nse")
    print("nse rows:", len(df), flush=True)
    isins = set(df["isin"].unique())
    orphan = sum(1 for isin in actions_by_isin if isin not in isins)
    n_orphan_pairs = sum(len(v) for k, v in actions_by_isin.items() if k not in isins)
    if orphan:
        print(f"WARNING nse: {orphan} action ISINs ({n_orphan_pairs} actions) "
              f"have no price rows -- dropped uncounted", flush=True)
    df, recon = apply_adjustments(df, actions_by_isin)
    atomic_write_parquet(df, os.path.join(FINAL, "nse_equity_2015_2026.parquet"))
    print("nse saved.", flush=True)

    atomic_write_parquet(recon, os.path.join(FINAL, "corporate_actions_reconciliation.parquet"))
    ok = recon["recon_ok"].mean() if len(recon) else float("nan")
    print(f"reconciliation: {len(recon)} capital actions checked, {ok:.1%} within 12% tolerance")

    # undocumented gaps (NSE)
    df = pd.read_parquet(os.path.join(FINAL, "nse_equity_2015_2026.parquet"))
    g = undocumented_gaps(df)
    g["exchange"] = "NSE"
    gaps = g[["exchange", "isin", "symbol", "series", "date", "close", "adj_close",
              "adj_cap_factor", "volume"]]
    print("nse undocumented >25% residual gaps:", len(gaps))
    atomic_write_csv(gaps, os.path.join(REPORT, "undocumented_gaps.csv"))


if __name__ == "__main__":
    main()
