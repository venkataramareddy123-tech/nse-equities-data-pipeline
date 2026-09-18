"""Parse all raw bhavcopy/delivery files into a unified, ISIN-keyed schema.

Output: data/interim/nse/{year}.parquet
Unified columns:
  date, exchange, symbol, series, isin, name,
  open, high, low, close, prev_close, last, avg_price,
  volume, turnover, trades, deliv_qty, deliv_pct
"""
import glob
import io
import os
import sys
import zipfile
from datetime import datetime

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_COMMON = os.path.join(_ROOT, "pipeline", "common")
for _p in [_ROOT, _COMMON]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from pipeline.common._util import atomic_write_parquet, free_ram_gb, parquet_probe_ok
except ImportError:
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_parquet, free_ram_gb, parquet_probe_ok


BASE = os.path.join(_ROOT, "data")
RAW = os.path.join(BASE, "raw")
INTERIM = os.path.join(BASE, "interim")

NSE_SERIES = {"EQ"}  # EQ-only universe: set before merging, every run.

COLS = ["date", "exchange", "symbol", "series", "isin", "name",
        "open", "high", "low", "close", "prev_close", "last", "avg_price",
        "volume", "turnover", "trades", "deliv_qty", "deliv_pct"]


def _read_zip_or_csv(path, **kw):
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            name = z.namelist()[0]
            data = z.open(name).read()
        return pd.read_csv(io.BytesIO(data), **kw)
    return pd.read_csv(path, **kw)


def num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False).str.strip(),
                         errors="coerce")


def parse_date_any(s):
    s = s.astype(str).str.strip()
    out = pd.to_datetime(s, format="%d-%b-%Y", errors="coerce")
    for fmt in ("%Y-%m-%d", "%d-%b-%y"):  # ISO + 2-digit-year (e.g. 13-Jul-20)
        m = out.isna()
        if not m.any():
            break
        out2 = pd.to_datetime(s[m], format=fmt, errors="coerce")
        out.loc[m] = out2
    return out


def base_frame(df):
    df = df.copy()
    for c in COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[COLS]


# ---------- NSE cm (old format, 2015 -> 2024-07-05) ----------
def parse_nse_cm(path):
    df = _read_zip_or_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df = df[df["SERIES"].astype(str).str.strip().isin(NSE_SERIES)]
    out = pd.DataFrame({
        "date": parse_date_any(df["TIMESTAMP"]),
        "exchange": "NSE",
        "symbol": df["SYMBOL"].astype(str).str.strip(),
        "series": df["SERIES"].astype(str).str.strip(),
        "isin": df["ISIN"].astype(str).str.strip(),
        "name": pd.NA,
        "open": num(df["OPEN"]), "high": num(df["HIGH"]),
        "low": num(df["LOW"]), "close": num(df["CLOSE"]),
        "prev_close": num(df["PREVCLOSE"]), "last": num(df["LAST"]),
        "avg_price": df["TOTTRDVAL"] / df["TOTTRDQTY"].replace(0, pd.NA),
        "volume": num(df["TOTTRDQTY"]), "turnover": num(df["TOTTRDVAL"]),
        "trades": num(df["TOTALTRADES"]),
    })
    return out


# ---------- NSE UDiFF (2024-07-08 ->) ----------
def parse_nse_udiff(path):
    df = _read_zip_or_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if "FinInstrmTp" in df.columns:
        df = df[df["FinInstrmTp"].astype(str).str.strip() == "STK"]
    df = df[df["SctySrs"].astype(str).str.strip().isin(NSE_SERIES)]
    out = pd.DataFrame({
        "date": parse_date_any(df["TradDt"]),
        "exchange": "NSE",
        "symbol": df["TckrSymb"].astype(str).str.strip(),
        "series": df["SctySrs"].astype(str).str.strip(),
        "isin": df["ISIN"].astype(str).str.strip(),
        "name": df.get("FinInstrmNm"),
        "open": num(df["OpnPric"]), "high": num(df["HghPric"]),
        "low": num(df["LwPric"]), "close": num(df["ClsPric"]),
        "prev_close": num(df["PrvsClsgPric"]), "last": num(df["LastPric"]),
        "avg_price": df["TtlTrfVal"] / df["TtlTradgVol"].replace(0, pd.NA),
        "volume": num(df["TtlTradgVol"]), "turnover": num(df["TtlTrfVal"]),
        "trades": num(df.get("TtlNbOfTxsExctd")),
    })
    return out


# ---------- NSE sec_bhavdata_full (2019-09-30 ->, has delivery) ----------
def parse_nse_full(path):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    df = df[df["SERIES"].isin(NSE_SERIES)]
    out = pd.DataFrame({
        "date": parse_date_any(df["DATE1"]),
        "exchange": "NSE",
        "symbol": df["SYMBOL"],
        "series": df["SERIES"],
        "isin": pd.NA,
        "name": pd.NA,
        "open": num(df["OPEN_PRICE"]), "high": num(df["HIGH_PRICE"]),
        "low": num(df["LOW_PRICE"]), "close": num(df["CLOSE_PRICE"]),
        "prev_close": num(df["PREV_CLOSE"]), "last": num(df["LAST_PRICE"]),
        "avg_price": num(df["AVG_PRICE"]),
        "volume": num(df["TTL_TRD_QNTY"]), "turnover": num(df["TURNOVER_LACS"]) * 1e5,
        "trades": num(df["NO_OF_TRADES"]),
        "deliv_qty": num(df["DELIV_QTY"]),
        "deliv_pct": num(df["DELIV_PER"]),
    })
    return out


# ---------- NSE MTO delivery (2015 ->) ----------
def mto_deliv(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return None
    lines = text.splitlines()
    date = None
    for l in lines[:4]:
        if l.startswith("Trade Date"):
            try:
                date = datetime.strptime(l.split("<")[1].split(">")[0], "%d-%b-%Y").date()
            except (IndexError, ValueError):
                return None
    if date is None:
        return None
    # vectorized: filter record lines, parse numerics in C (was a Python float() loop)
    body = [l for l in lines if l.startswith("20,")]
    if not body:
        return None
    try:
        df = pd.read_csv(io.StringIO("\n".join(body)), header=None,
                         names=["_t", "_n", "symbol", "series",
                                "qty_traded", "deliv_qty", "deliv_pct"])
    except Exception:
        return None
    for c in ("qty_traded", "deliv_qty", "deliv_pct"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["series"] = df["series"].astype(str).str.strip()
    df = df.dropna(subset=["symbol", "series", "qty_traded", "deliv_qty", "deliv_pct"])
    df = df[df["series"].isin(NSE_SERIES)]
    if not len(df):
        return None
    df["date"] = pd.Timestamp(date)
    return df[["date", "symbol", "series", "qty_traded",
               "deliv_qty", "deliv_pct"]].reset_index(drop=True)


def files_for_year(kind, year):
    if kind == "cm":
        ex, sub = "nse", "cm"
    elif kind == "udiff":
        ex, sub = "nse", "udiff"
    elif kind == "full":
        ex, sub = "nse", "full"
    elif kind == "mto":
        ex, sub = "nse", "mto"
    else:
        raise ValueError(kind)
    pat = os.path.join(RAW, ex, sub, str(year), "*")
    # ignore quarantine/partial downloads (*.bad, *.part) -- repair scan owns those
    return sorted(p for p in glob.glob(pat)
                  if not p.lower().endswith((".bad", ".part")))


def _max_mtime(paths):
    best = 0.0
    for p in paths:
        try:
            t = os.path.getmtime(p)
            if t > best:
                best = t
        except OSError:
            pass
    return best


def year_is_fresh(year):
    """True if interim parquet is newer than every raw file for that year.

    Probes readability too: a truncated interim (crash mid-write) is never
    trusted, no matter how new its mtime is.
    """
    nse_out = os.path.join(INTERIM, "nse", f"{year}.parquet")
    if not parquet_probe_ok(nse_out):
        return False
    raws = (files_for_year("cm", year) + files_for_year("udiff", year)
            + files_for_year("full", year) + files_for_year("mto", year))
    if not raws:
        return True
    try:
        out_m = os.path.getmtime(nse_out)
    except OSError:
        return False
    return out_m >= _max_mtime(raws)


def parse_nse_year(year):
    """Build the NSE interim frame for one year (price + delivery merge).

    Pure function of raw files on disk -> (df, fails). Years are independent,
    so main() runs these in parallel worker processes.
    """
    fails = []
    frames = []
    for p in files_for_year("cm", year) + files_for_year("udiff", year):
        is_udiff = os.path.sep + "udiff" + os.path.sep in p or "BhavCopy_NSE" in p
        try:
            fn = parse_nse_udiff(p) if is_udiff else parse_nse_cm(p)
            frames.append(fn)
        except Exception as e:  # noqa: BLE001
            fails.append(("NSE parse FAIL", os.path.basename(p),
                          type(e).__name__, str(e)))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS)
    df = df.drop_duplicates(subset=["date", "symbol", "series"], keep="first")

    # delivery: sec_bhavdata_full (2019-09-30+) primary; MTO fills the rest
    full_frames = []
    for p in files_for_year("full", year):
        try:
            full_frames.append(parse_nse_full(p))
        except Exception as e:  # noqa: BLE001
            fails.append(("NSE full FAIL", os.path.basename(p),
                          type(e).__name__, str(e)))
    if full_frames:
        fdf = pd.concat(full_frames, ignore_index=True)
        fdf = fdf.drop_duplicates(subset=["date", "symbol", "series"], keep="first")
        fdel = fdf[["date", "symbol", "series", "deliv_qty", "deliv_pct"]]
        df = df.merge(fdel, on=["date", "symbol", "series"], how="left",
                      suffixes=("", "_full"))
        for c in ["deliv_qty", "deliv_pct"]:
            if f"{c}_full" in df.columns:
                df[c] = df[c].combine_first(df[f"{c}_full"])
                df.drop(columns=[f"{c}_full"], inplace=True)

    mfs = []
    for p in files_for_year("mto", year):
        try:
            d = mto_deliv(p)
            if d is not None and len(d):
                mfs.append(d)
        except Exception as e:  # noqa: BLE001
            fails.append(("MTO FAIL", os.path.basename(p), type(e).__name__, str(e)))
    mdf = pd.concat(mfs, ignore_index=True) if mfs else None
    if mdf is not None:
        mdel = mdf[["date", "symbol", "series", "deliv_qty", "deliv_pct"]]
        mdel = mdel.drop_duplicates(subset=["date", "symbol", "series"], keep="first")
        df = df.merge(mdel, on=["date", "symbol", "series"], how="left",
                      suffixes=("", "_mto"))
        for c in ["deliv_qty", "deliv_pct"]:
            if f"{c}_mto" in df.columns:
                df[c] = df[c].combine_first(df[f"{c}_mto"])
                df.drop(columns=[f"{c}_mto"], inplace=True)

    df = base_frame(df).sort_values(["date", "symbol", "series"]).reset_index(drop=True)
    return df, fails


def _pool_map(fn, years, workers):
    if workers <= 1:
        return {y: fn(y) for y in years}
    from concurrent.futures import ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return dict(zip(years, ex.map(fn, years)))


def main():
    import datetime as _dt
    _today = _dt.date.fromisoformat(os.environ["ROLL_TODAY"]) if os.environ.get("ROLL_TODAY") else _dt.date.today()
    _last_year = _today.year  # rolling: always parse through current year
    _force = os.environ.get("ROLL_FORCE_YEARS", "")
    _force_all = os.environ.get("ROLL_FULL") == "1" or _force.strip().lower() == "all"
    _force_years = set()
    if _force and not _force_all:
        for tok in _force.split(","):
            tok = tok.strip()
            if tok.isdigit():
                _force_years.add(int(tok))
    os.makedirs(os.path.join(INTERIM, "nse"), exist_ok=True)
    stats = []
    print("startup: cm2015 files:", len(files_for_year("cm", 2015)),
          "| mto2015:", len(files_for_year("mto", 2015)),
          f"| full{_last_year}:", len(files_for_year("full", _last_year)), flush=True)

    todo = [y for y in range(2015, _last_year + 1)
            if _force_all or y in _force_years or not year_is_fresh(y)]
    for y in range(2015, _last_year + 1):
        if y not in todo:
            print(f"SKIP {y} (interim newer than raw)", flush=True)

    import multiprocessing as _mp
    if len(todo) > 1:
        workers = max(1, min(4, _mp.cpu_count() or 2, len(todo),
                             int(free_ram_gb() // 1.5)))
    else:
        workers = 1
    if workers > 1:
        print(f"parallel parse: {len(todo)} years x {workers} workers", flush=True)
    nse_results = _pool_map(parse_nse_year, todo, workers)
    fail_status = {}
    for year in todo:
        df, fails = nse_results[year]
        for f in fails:
            print(*f)
        if fails:
            fail_status[str(year)] = len(fails)
        out = os.path.join(INTERIM, "nse", f"{year}.parquet")
        atomic_write_parquet(df, out)
        stats.append(("nse", year, len(df), df["date"].nunique() if len(df) else 0))
        print("NSE", year, len(df), "rows,", df["date"].nunique(), "dates", flush=True)

    # machine-readable fail counts: roll_update gates on current-year failures
    # so a silently skipped raw file can never pass as a healthy rebuild
    import json as _json
    # pyrefly: ignore [missing-import]
    from _util import atomic_write_json
    atomic_write_json(fail_status, os.path.join(INTERIM, "parse_status.json"))
    print("ALL DONE")


if __name__ == "__main__":
    main()
