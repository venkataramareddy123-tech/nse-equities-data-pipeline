"""Probe NSE archive formats across the available historical periods."""
import datetime as dt
import sys

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

MON = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
       "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def url_nse_old(d: dt.date) -> str:
    return (f"https://archives.nseindia.com/content/historical/EQUITIES/"
            f"{d.year}/{MON[d.month-1]}/cm{d:%d}{MON[d.month-1]}{d.year}bhav.csv")


def url_nse_new(d: dt.date) -> str:
    return (f"https://nsearchives.nseindia.com/content/cm/"
            f"BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.CSV")


def url_nse_full(d: dt.date) -> str:
    return f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"


def url_nse_mto(d: dt.date) -> str:
    return f"https://archives.nseindia.com/archives/equities/mto/MTO_{d:%d%m%Y}.DAT"


def probe(name: str, url: str, hdrs=None) -> None:
    try:
        r = requests.get(url, headers=hdrs or HEADERS, timeout=25)
        if r.status_code == 200 and len(r.content) > 500:
            first = r.text[:400].replace("\n", " | ").replace("\r", "")
            print(f"OK   {name:12s} {url.split('/')[-1]:45s} "
                  f"{len(r.content):>9,}B  {first[:180]}")
        else:
            print(f"MISS {name:12s} {url.split('/')[-1]:45s} "
                  f"HTTP {r.status_code} len={len(r.content)}")
    except Exception as e:
        print(f"ERR  {name:12s} {url.split('/')[-1]:45s} "
              f"{type(e).__name__}: {e}")


def first_monday(year: int, month: int) -> dt.date:
    d = dt.date(year, month, 1)
    while d.weekday() > 3:
        d += dt.timedelta(days=1)
    return d


if __name__ == "__main__":
    for y in range(2014, 2027):
        d = first_monday(y, 1)
        print(f"--- {y} ({d}) ---")
        probe("NSE_old", url_nse_old(d))
        if y >= 2024:
            probe("NSE_new", url_nse_new(d))
        probe("NSE_full", url_nse_full(d))
        probe("NSE_mto", url_nse_mto(d))
        sys.stdout.flush()
