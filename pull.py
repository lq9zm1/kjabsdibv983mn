#!/usr/bin/env python3
"""
pull.py  —  EODHD full-history price puller (drop-in replacement for the yfinance version).

Returns a DataFrame matching the BigQuery price_history schema:
    ticker, date, open, high, low, close, adj_close, volume

Contract preserved for run_nightly.py:
    pull_prices(tickers) -> (DataFrame, failed_list)
    to_yahoo(t)          -> EODHD symbol mapper (kept same name so run_nightly's
                            metadata step `yf.Ticker(pullmod.to_yahoo(t))` still calls it;
                            see NOTE at bottom about the .info metadata step)

KEY DIFFERENCES vs yfinance:
  - One EODHD call per ticker = full history (period=d). ~3,062 calls/night = ~3% of
    the 100,000/day limit. (Bulk-last-day incremental is a LATER optimization.)
  - EODHD adjusted_close is split+dividend adjusted (matches TradingView). We map it
    to adj_close. EODHD 'close' is the raw/unadjusted close -> close.
  - US tickers use the .US suffix. Dotted tickers (BRK.B) -> BRK-B.US on EODHD.

SETUP:
  - requires: requests, pandas
  - API key from env var EODHD_API_KEY  (GitHub Actions secret), falls back to a
    local constant for manual runs.
"""

import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# ---- config -----------------------------------------------------------------
# Key resolution order: env var (GitHub Actions secret) -> local constant.
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "PASTE_KEY_FOR_LOCAL_RUNS")
BASE = "https://eodhd.com/api/eod"
FROM = "1900-01-01"            # full history; EODHD returns from inception
PAUSE_SEC = 0.05              # gentle spacing; 1000 req/min limit = 16/s, this is well under
TIMEOUT = 60
RETRY = 2                      # per-ticker retries on transient errors
COLS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]


def to_yahoo(t: str) -> str:
    """EODHD symbol for a US ticker. (Name kept for run_nightly compatibility.)
    BRK.B -> BRK-B.US ; dotted/class shares use '-' like Yahoo, plus '.US' exchange."""
    return t.replace(".", "-") + ".US"


def _pull_one(eod_symbol):
    """Return list-of-dict rows for one EODHD symbol, or None on failure."""
    url = f"{BASE}/{eod_symbol}"
    params = {"api_token": EODHD_API_KEY, "from": FROM, "period": "d", "fmt": "json"}
    for attempt in range(RETRY + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []
            # 404 = symbol not on EODHD; don't retry
            if r.status_code == 404:
                return None
            # 429/5xx = transient; back off and retry
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


def _frame(rows, original_ticker):
    """EODHD JSON rows -> price_history-schema DataFrame for one ticker."""
    recs = []
    for bar in rows:
        try:
            recs.append({
                "ticker":    original_ticker,
                "date":      bar["date"],                       # 'YYYY-MM-DD'
                "open":      float(bar["open"]),
                "high":      float(bar["high"]),
                "low":       float(bar["low"]),
                "close":     float(bar["close"]),               # raw close
                "adj_close": float(bar["adjusted_close"]),      # split+div adjusted (TV-match)
                "volume":    bar.get("volume", 0),
            })
        except (KeyError, TypeError, ValueError):
            continue
    if not recs:
        return None
    return pd.DataFrame.from_records(recs)


def pull_prices(tickers):
    """Pull full-history OHLCV for a list of (already-cleaned) ticker strings.
    Returns (DataFrame, failed_list_of_original_tickers)."""
    if EODHD_API_KEY in ("", "PASTE_KEY_FOR_LOCAL_RUNS") and "EODHD_API_KEY" not in os.environ:
        print("WARNING: EODHD_API_KEY not set (env var or local constant). Calls will 401.",
              flush=True)

    emap = {to_yahoo(t): t for t in tickers}     # eodhd symbol -> original ticker
    frames, failed = [], []
    syms = list(emap.keys())
    total = len(syms)

    BATCH_LOG = max(1, total // 100)             # ~100 progress lines like the old batches
    for i, esym in enumerate(syms, 1):
        if i % BATCH_LOG == 0 or i == total:
            print(f"  {i}/{total} ...", flush=True)
        rows = _pull_one(esym)
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
