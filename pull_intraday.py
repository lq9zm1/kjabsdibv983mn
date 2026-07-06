"""
pull_intraday.py — SIMPLE recent-window EODHD intraday puller (quota-guarded) -> BigQuery price_intraday.
Pulls the last N calendar days of 5m bars for a fixed 20-ticker watchlist.

🚨 FIRM QUOTA STANDARD: hard cap MAX_API_CALLS; NEVER rely on the 500 buffer; respect 1000/min.
🔐 SECURITY: API token read from env var EODHD_API_KEY — never hardcode / never paste in chat.

Run: EODHD_API_KEY=your_token python pull_intraday.py
Requires: requests, pandas, google-cloud-bigquery, db-dtypes
"""
import os
import time
import datetime as dt

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE = f"{PROJECT}.{DATASET}.price_intraday"
API_KEY = os.environ.get("EODHD_API_KEY")

INTERVAL = os.environ.get("INTRADAY_INTERVAL", "5m")   # 5m (600d history cap on EODHD)
LOOKBACK_DAYS = 14                                      # calendar days back (~10 trading days)

# ---------- FIRM QUOTA GUARDRAILS ----------
MAX_API_CALLS = 20000        # hard cap. NEVER touch the 500 buffer.
CALLS_PER_REQUEST = 5        # EODHD intraday = 5 API calls per request
SLEEP_SEC = 0.08             # ~750 req/min, under the 1000/min limit

# ---------- 20-TICKER WATCHLIST (edit here) ----------
TICKERS = [
    "HOOD", "AFRM", "OSCR", "ORBS", "BE", "AAOI", "NVDA", "INTC", "MU", "LITE",
    "SEZL", "BROS", "BB", "HIMS", "HUM", "PANW", "DELL", "ARM", "NBIS", "ALAB",
]


def pull_one(ticker, frm, to):
    url = f"https://eodhd.com/api/intraday/{ticker}.US"
    params = {"api_token": API_KEY, "interval": INTERVAL, "fmt": "json", "from": frm, "to": to}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["ticker"] = ticker
    df["interval"] = INTERVAL
    return df


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode the token).")
    client = bigquery.Client(project=PROJECT)

    now = dt.datetime.now(dt.timezone.utc)
    to = int(now.timestamp())
    frm = int((now - dt.timedelta(days=LOOKBACK_DAYS)).timestamp())

    calls = 0
    frames = []
    for i, tk in enumerate(TICKERS):
        if calls + CALLS_PER_REQUEST > MAX_API_CALLS:
            print(f"QUOTA CAP reached ({calls} calls) — aborting before {tk}.")
            break
        try:
            df = pull_one(tk, frm, to)
            calls += CALLS_PER_REQUEST
            print(f"  {tk}: {len(df)} bars (calls so far: {calls})")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  !! {tk}: {e}")
        time.sleep(SLEEP_SEC)

    if not frames:
        print("No data pulled.")
        return

    out = pd.concat(frames, ignore_index=True)
    # EODHD intraday fields: timestamp(unix), gmtoffset, datetime(str UTC), open, high, low, close, volume
    out["dt_utc"] = pd.to_datetime(out["timestamp"], unit="s", utc=True)
    # target_date = the trading day (UTC date of the bar) — kept for downstream grouping
    out["target_date"] = out["dt_utc"].dt.strftime("%Y-%m-%d")
    for c in ("open", "high", "low", "close", "volume"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    keep = ["ticker", "target_date", "interval", "dt_utc", "open", "high", "low", "close", "volume"]
    out = out[keep].dropna(subset=["open", "high", "low", "close"])
    # dedupe within this pull (belt-and-suspenders; TRUNCATE handles cross-run dupes)
    out = out.drop_duplicates(subset=["ticker", "dt_utc"])

    client.load_table_from_dataframe(
        out, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",   # ★ clean reload each run — no dup rows
            time_partitioning=bigquery.TimePartitioning(field="dt_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(out)} intraday bars -> price_intraday (TRUNCATE). API calls: {calls} (cap {MAX_API_CALLS}).")


if __name__ == "__main__":
    main()
