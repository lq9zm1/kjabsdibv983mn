#!/usr/bin/env python3
"""
pull.py  —  EODHD price puller.  Two modes:
    pull_prices(tickers)        -> (DataFrame, failed_list)   FULL history, 1 call/ticker.
                                   Used for the WEEKLY reset (Sunday) — self-heals splits/divs.
    pull_recent(tickers, days)  -> (DataFrame, complete_ok)   INCREMENTAL, last `days` cal days
                                   via eod-bulk-last-day/US (one request per day = whole US market,
                                   filtered to curated in code). Used on weekday nightlies.

Returns rows matching the BigQuery price_history schema:
    ticker, date, open, high, low, close, adj_close, volume

Contract preserved for run_nightly.py:
    pull_prices(tickers) -> (DataFrame, failed_list)
    to_yahoo(t)          -> EODHD symbol mapper (kept same name; run_nightly's metadata step
                            yf.Ticker(pullmod.to_yahoo(t)) still calls it).

KEY POINTS:
  - EODHD adjusted_close is split+dividend adjusted (matches TradingView) -> adj_close.
    'close' is the raw/unadjusted close -> close.
  - US tickers use the .US suffix for the per-ticker EOD endpoint. Dotted tickers (BRK.B) -> BRK-B.
  - eod-bulk-last-day/US costs 100 API units per request; 7 days = 700 units (vs a 100k/day limit).

API key from env EODHD_API_KEY (GitHub Actions secret), falls back to a local constant.
"""
import os
import re
import sys
import time
from datetime import date as _date, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---- config -----------------------------------------------------------------
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "PASTE_KEY_FOR_LOCAL_RUNS")
BASE = "https://eodhd.com/api/eod"
BULK = "https://eodhd.com/api/eod-bulk-last-day/US"
FROM = "1900-01-01"            # full history; EODHD returns from inception
PAUSE_SEC = 0.05
TIMEOUT = 60
RETRY = 2                      # per-request retries on transient errors
COLS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]


def _norm(t: str) -> str:
    return str(t).upper().strip().replace(".", "-").replace("/", "-")


def to_yahoo(t: str) -> str:
    """EODHD symbol for a US ticker. (Name kept for run_nightly compatibility.)"""
    return t.replace(".", "-") + ".US"


# ============================================================================
# FULL history (weekly reset)
# ============================================================================
def _pull_one(eod_symbol, from_date=FROM):
    url = f"{BASE}/{eod_symbol}"
    params = {"api_token": EODHD_API_KEY, "from": from_date, "period": "d", "fmt": "json"}
    for attempt in range(RETRY + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []
            if r.status_code == 404:
                return None
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


def _frame(rows, original_ticker):
    recs = []
    for bar in rows:
        try:
            recs.append({
                "ticker":    original_ticker,
                "date":      bar["date"],
                "open":      float(bar["open"]),
                "high":      float(bar["high"]),
                "low":       float(bar["low"]),
                "close":     float(bar["close"]),
                "adj_close": float(bar["adjusted_close"]),
                "volume":    bar.get("volume", 0),
            })
        except (KeyError, TypeError, ValueError):
            continue
    if not recs:
        return None
    return pd.DataFrame.from_records(recs)


def pull_prices(tickers, from_date=FROM):
    """OHLCV for a list of tickers from `from_date` (default = full history). Returns (DataFrame, failed_list).
    Pass a recent from_date (e.g. ~2yr ago) for the weekly split/div self-heal so it doesn't re-fetch decades."""
    if EODHD_API_KEY in ("", "PASTE_KEY_FOR_LOCAL_RUNS") and "EODHD_API_KEY" not in os.environ:
        print("WARNING: EODHD_API_KEY not set. Calls will 401.", flush=True)

    emap = {to_yahoo(t): t for t in tickers}
    frames, failed = [], []
    syms = list(emap.keys())
    total = len(syms)
    BATCH_LOG = max(1, total // 100)
    for i, esym in enumerate(syms, 1):
        if i % BATCH_LOG == 0 or i == total:
            print(f"  {i}/{total} ...", flush=True)
        rows = _pull_one(esym, from_date)
        if rows is None:
            failed.append(esym); continue
        fr = _frame(rows, emap[esym])
        if fr is None or fr.empty:
            failed.append(esym); continue
        frames.append(fr)
        time.sleep(PAUSE_SEC)

    if not frames:
        return pd.DataFrame(columns=COLS), sorted({emap.get(f, f) for f in set(failed)})
    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["close"])
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")
    out = out[COLS]
    failed_orig = sorted({emap.get(f, f) for f in set(failed)})
    return out, failed_orig


# ============================================================================
# INCREMENTAL (nightly) — last N days via eod-bulk-last-day/US
# ============================================================================
def _bulk_one_day(day):
    """One eod-bulk-last-day/US request for a date. Returns list of row dicts (or None on failure)."""
    params = {"api_token": EODHD_API_KEY, "date": day.isoformat(), "fmt": "json"}
    for attempt in range(RETRY + 1):
        try:
            r = requests.get(BULK, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None                                  # all retries failed


def pull_recent(tickers, days=7):
    """Last `days` calendar days for the curated tickers, via bulk-last-day (1 request/day).
    Returns (DataFrame in COLS schema, complete_ok). complete_ok=False if the fetch looks
    incomplete (a day's request failed, or too few tickers came back) so the caller can ABORT
    rather than write partial data."""
    if EODHD_API_KEY in ("", "PASTE_KEY_FOR_LOCAL_RUNS") and "EODHD_API_KEY" not in os.environ:
        print("WARNING: EODHD_API_KEY not set. Calls will 401.", flush=True)

    keep = {_norm(t) for t in tickers}
    frames, failures = [], 0
    today = _date.today()
    for d in range(days):
        day = today - timedelta(days=d)
        if day.weekday() >= 5:                   # skip Sat/Sun (no trading -> empty)
            continue
        rows = _bulk_one_day(day)
        if rows is None:                         # request failed after retries
            failures += 1
            print(f"  bulk {day} FAILED after retries", flush=True)
            continue
        recs = []
        for b in rows:
            code = _norm(b.get("code", ""))
            if code not in keep:
                continue
            try:
                recs.append({
                    "ticker": code, "date": b["date"],
                    "open": float(b["open"]), "high": float(b["high"]),
                    "low": float(b["low"]), "close": float(b["close"]),
                    "adj_close": float(b["adjusted_close"]), "volume": b.get("volume", 0),
                })
            except (KeyError, TypeError, ValueError):
                continue
        if recs:
            frames.append(pd.DataFrame.from_records(recs))
            print(f"  bulk {day}: {len(recs)} curated rows", flush=True)
        time.sleep(PAUSE_SEC)

    if not frames:
        return pd.DataFrame(columns=COLS), False
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker", "date"])
    out = out.dropna(subset=["close"])
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")
    out = out[COLS]
    # completeness gate: NO failed days, and a healthy fraction of curated came back.
    complete_ok = (failures == 0) and (out["ticker"].nunique() >= 0.5 * max(1, len(keep)))
    return out, complete_ok


def _load_tickers(path):
    raw = Path(path).read_text()
    toks = re.split(r"[^A-Za-z0-9.\-]+", raw.upper())
    seen, out = set(), []
    for t in toks:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out


if __name__ == "__main__":
    tk = _load_tickers("tickers.txt")
    print(f"{len(tk)} unique tickers loaded.")
    df, failed = pull_prices(tk)
    df.to_csv("price_history.csv", index=False)
    print(f"\nWrote {len(df):,} rows for {df['ticker'].nunique()} tickers -> price_history.csv")
    if failed:
        print(f"{len(failed)} symbols returned no data: " + ", ".join(failed[:50])
              + (" ..." if len(failed) > 50 else ""))
