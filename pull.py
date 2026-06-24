#!/usr/bin/env python3
"""
pull.py
Stability-patched Yahoo Finance price puller.
- threads OFF, small batches, pauses, fresh tz cache, single-threaded retry.
Returns a DataFrame matching the BigQuery price_history schema:
    ticker, date, open, high, low, close, adj_close, volume

Can be imported (pull_prices(tickers)) or run standalone (reads tickers.txt,
writes price_history.csv).
"""

import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

PERIOD = "max"   # was "2y"
CHUNK     = 20
PAUSE_SEC = 2
COLS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]


def to_yahoo(t: str) -> str:
    return t.replace(".", "-")


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _row_frame(sub, original_ticker):
    df = sub.reset_index()[
        ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    ].copy()
    df.columns = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    df.insert(0, "ticker", original_ticker)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def pull_prices(tickers):
    """Pull OHLCV for a list of (already-cleaned) ticker strings.
    Returns (DataFrame, failed_list)."""
    try:
        os.makedirs("./.yf_cache", exist_ok=True)
        yf.set_tz_cache_location("./.yf_cache")
    except Exception:
        pass

    ymap = {to_yahoo(t): t for t in tickers}          # yahoo symbol -> original
    frames, failed = [], []
    batches = list(_chunks(list(ymap.keys()), CHUNK))
    total = len(batches)

    for bi, batch in enumerate(batches, 1):
        print(f"  batch {bi}/{total} ...", flush=True)
        try:
            data = yf.download(
                batch, period=PERIOD, interval="1d",
                auto_adjust=False, group_by="ticker",
                threads=False, progress=False,
            )
        except Exception as e:
            print("  batch error:", e)
            failed += batch
            continue
        for ysym in batch:
            try:
                sub = data if len(batch) == 1 else data[ysym]
                sub = sub.dropna(how="all")
                if sub.empty:
                    failed.append(ysym); continue
                frames.append(_row_frame(sub, ymap[ysym]))
            except Exception:
                failed.append(ysym)
        time.sleep(PAUSE_SEC)

    # single-threaded retry pass (catches batch-mode drops)
    ok = set()
    if failed:
        retry = sorted(set(failed)); failed = []
        print(f"Retrying {len(retry)} symbols individually...", flush=True)
        for ysym in retry:
            try:
                sub = yf.download(ysym, period=PERIOD, interval="1d",
                                  auto_adjust=False, threads=False,
                                  progress=False).dropna(how="all")
                if sub.empty:
                    failed.append(ysym); continue
                frames.append(_row_frame(sub, ymap[ysym]))
                ok.add(ysym)
            except Exception:
                failed.append(ysym)
            time.sleep(0.5)

    if not frames:
        return pd.DataFrame(columns=COLS), sorted(set(failed))

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["close"])
    out["volume"] = out["volume"].fillna(0).astype("int64")
    out = out[COLS]
    # map failed yahoo symbols back to original tickers for the caller
    failed_orig = sorted({ymap.get(f, f) for f in set(failed)})
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
        print(f"{len(failed)} symbols returned no data: " + ", ".join(failed))
