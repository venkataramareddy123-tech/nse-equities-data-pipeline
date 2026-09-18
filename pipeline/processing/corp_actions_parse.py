"""Parse NSE corporate action feeds into a structured table.

Output: data/interim/corporate_actions.parquet
Columns: source, isin, symbol, ex_date, record_date, type,
         a_num, a_den (bonus/rights new:held), rights_price,
         fv_old, fv_new (split/consolidation), div_amount, div_pct, fv,
         is_reorg, purpose_raw
"""
import glob
import json
import os
import re
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from pipeline.common._util import atomic_write_parquet
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_parquet

BASE = os.path.join(_ROOT, "data")
OUT = os.path.join(BASE, "interim", "corporate_actions.parquet")


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _norm(txt):
    t = (txt or "").lower()
    t = t.replace("\u2013", "-").replace("\u2014", "-")
    t = t.replace("/-", " ").replace("/ -", " ").replace("per share", " ")
    t = re.sub(r"(?<![a-z])(?:rs\.?|re\.?|inr|₹)(?![a-z])\s*", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def parse_text(txt):
    """Return dict of parsed components from a purpose/subject string."""
    raw = (txt or "").lower()
    t = _norm(txt)
    is_pref = ("preference" in raw or "pref" in raw
               or "debenture" in raw or "bond" in raw)
    out = {"type": set(), "a_num": None, "a_den": None, "rights_price": None,
           "fv_old": None, "fv_new": None, "div_amount": None, "div_pct": None,
           "is_reorg": False, "parsed_ok": True}

    # ---- bonus: "1:1 bonus", "bonus 2:5" ----
    m = re.search(r"bonus[^0-9]{0,40}?(\d+)\s*[:\-]\s*(\d+)", t) or \
        re.search(r"(\d+)\s*[:\-]\s*(\d+)\s*bonus", t)
    if m and not is_pref:
        out["type"].add("bonus")
        out["a_num"], out["a_den"] = float(m.group(1)), float(m.group(2))

    # ---- split / sub-division: "fv split from 10 to 1" ----
    if re.search(r"split|sub[- ]?divis", t):
        out["type"].add("split")
        t2 = t.replace("face value", "fv")
        m = re.search(r"(?:from|f/)\s*(\d+(?:\.\d+)?)\s*(?:to|-)\s*(?:fv\s*)?(\d+(?:\.\d+)?)", t2)
        if m:
            out["fv_old"], out["fv_new"] = float(m.group(1)), float(m.group(2))
        else:
            out["parsed_ok"] = False

    # ---- consolidation (reverse split): FV up ----
    if "consolidat" in t:
        out["type"].add("consolidation")
        m = re.search(r"from\s*(\d+(?:\.\d+)?)\s*(?:to|-)\s*(?:fv\s*)?(\d+(?:\.\d+)?)", t)
        if m:
            out["fv_old"], out["fv_new"] = float(m.group(1)), float(m.group(2))
        else:
            out["parsed_ok"] = False

    # ---- rights: "91:10 @ premium rs 10" or "1:4 at rs 50" ----
    if "right" in raw and ("issue" in raw or "entitlement" in raw or
                           re.search(r"\d+\s*[:\-]\s*\d+", t)):
        m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", t)
        if m:
            out["type"].add("rights")
            out["a_num"], out["a_den"] = float(m.group(1)), float(m.group(2))
            mp = re.search(r"(?:at|@)\s*(?:a\s*premium\s*of\s*)?(premium\s*)?\.?\s*(\d+(?:\.\d+)?)", t)
            if mp:
                price = float(mp.group(2))
                if mp.group(1):
                    out["_premium"] = price  # resolved to fv+premium by caller when fv known
                else:
                    out["rights_price"] = price
            else:
                out["parsed_ok"] = False
        else:
            out["parsed_ok"] = False

    # ---- dividend ----
    if "dividend" in raw:
        out["type"].add("dividend")
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*%", t)
        if m2:
            out["div_pct"] = float(m2.group(1))
        if "%" not in t and not m2:
            md = re.search(r"dividend[^0-9]{0,30}(\d+(?:\.\d+)?)", t) or \
                 re.search(r"(?:rs|re)\s*[-:.]?\s*(\d+(?:\.\d+)?)", t)
            if md:
                out["div_amount"] = float(md.group(1))
        if out["div_amount"] is None and out["div_pct"] is None:
            out["parsed_ok"] = False

    # ---- reorgs (manual review; no auto adjustment) ----
    if re.search(r"scheme of arrangement|demerger|de-merger|amalgamation|reduction of capital"
                 r"|capital reduction|buy ?back|merger|composite scheme|restructuring", raw):
        out["type"].add("reorg")
        out["is_reorg"] = True

    out["type"] = "|".join(sorted(out["type"]))
    return out


def parse_nse_chunks():
    rows = []
    for fp in sorted(glob.glob(os.path.join(BASE, "raw", "nse", "corp_actions", "ca_*.json"))):
        try:
            data = json.load(open(fp, encoding="utf-8"))
        except Exception:
            continue
        for x in data:
            if x.get("series") not in ("EQ", "BE", "BZ", "SM", "ST", "SZ", "IL", "IQ"):
                continue
            p = parse_text(x.get("subject"))
            if p.get("_premium") is not None:
                fv = _num(x.get("faceVal"))
                if fv:
                    p["rights_price"] = fv + p["_premium"]
                p.pop("_premium", None)
            rows.append({
                "source": "NSE", "isin": (x.get("isin") or "").strip(),
                "symbol": (x.get("symbol") or "").strip(),
                "ex_date": pd.to_datetime(x.get("exDate"), format="%d-%b-%Y", errors="coerce"),
                "record_date": pd.to_datetime(x.get("recDate"), format="%d-%b-%Y", errors="coerce"),
                "fv": _num(x.get("faceVal")),
                "purpose_raw": x.get("subject"),
                **p,
            })
    return pd.DataFrame(rows)


def main():
    nse = parse_nse_chunks()
    ca = nse.dropna(subset=["ex_date"]).copy()
    ca = ca.drop_duplicates(subset=["source", "isin", "ex_date", "purpose_raw"])
    ca = ca.sort_values(["isin", "ex_date"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    atomic_write_parquet(ca, OUT)
    print("actions:", len(ca), "| NSE:", len(nse))
    print(ca["type"].value_counts().to_string())
    print("unparsed-ish:", (~ca["parsed_ok"]).sum())


if __name__ == "__main__":
    main()
