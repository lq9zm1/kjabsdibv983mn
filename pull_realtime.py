"""
pull_realtime.py — EODHD /real-time REST snapshot (Layer 2) -> BigQuery realtime_quotes.
Reads symbols from v_ready_intraday_symbols. Batched multi-ticker call. WRITE_TRUNCATE (latest snapshot only).
Gives live price / %today / day high-low for live trigger distances. History = Layer-1 1m bars.

FIRM QUOTA: hard cap MAX_API_CALLS. SECURITY: token from env EODHD_API_KEY (never hardcode).
Run: EODHD_API_KEY=your_token python pull_realtime.py
Requires: requests, pandas, google-cloud-bigquery, db-dtypes
"""
import os
import datetime as dt

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE   = f"{PROJECT}.{DATASET}.realtime_quotes"
META    = f"{PROJECT}.{DATASET}.realtime_meta"
SYM_VIEW = f"{PROJECT}.{DATASET}.v_ready_intraday_symbols"
API_KEY = os.environ.get("EODHD_API_KEY")

BATCH = 15          # EODHD real-time multi-ticker: primary + up to ~15 in s=
MAX_API_CALLS = 20000


def fetch_batch(primary, others):
    url = f"https://eodhd.com/api/real-time/{primary}.US"
    params = {"api_token": API_KEY, "fmt": "json"}
    if others:
        params["s"] = ",".join(f"{s}.US" for s in others)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    return js if isinstance(js, list) else [js]   # single ticker returns a dict


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode the token).")
    client = bigquery.Client(project=PROJECT)

    symbols = [r["symbol"] for r in client.query(
        f"SELECT symbol FROM `{SYM_VIEW}` ORDER BY symbol").result()]
    if not symbols:
        print("No symbols in v_ready_intraday_symbols.")
        return

    rows, calls = [], 0
    for i in range(0, len(symbols), BATCH):
        if calls + 1 > MAX_API_CALLS:
            print(f"QUOTA CAP ({calls}) — aborting."); break
        chunk = symbols[i:i + BATCH]
        try:
            data = fetch_batch(chunk[0], chunk[1:])
            calls += 1
            rows.extend(data)
        except Exception as e:
            print(f"  !! batch {chunk[0]}: {e}")

    if not rows:
        print("No quotes returned."); return

    df = pd.DataFrame(rows)
    # EODHD real-time fields: code, timestamp, gmtoffset, open, high, low, close, volume, previousClose, change, change_p
    df["ticker"] = df["code"].astype(str).str.replace(".US", "", regex=False)
    df["quote_utc"] = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"), unit="s", utc=True)
    for c in ("open", "high", "low", "close", "volume", "previousClose", "change", "change_p"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = ["ticker", "quote_utc", "open", "high", "low", "close", "volume",
            "previousClose", "change", "change_p"]
    df = df[[c for c in keep if c in df.columns]].dropna(subset=["close"]).drop_duplicates(subset=["ticker", "quote_utc"])

    client.load_table_from_dataframe(
        df, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",   # ★ snapshots accumulate = 15-min bar series (dedupe via v_realtime_bars)
            time_partitioning=bigquery.TimePartitioning(field="quote_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    now = dt.datetime.now(dt.timezone.utc)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    client.query(
        f"CREATE OR REPLACE TABLE `{META}` AS "
        f"SELECT TIMESTAMP('{ts} UTC') AS last_updated_utc, {len(df)} AS n_quotes, {calls} AS api_calls"
    ).result()
    print(f"Done: {len(df)} quotes -> realtime_quotes (TRUNCATE). calls: {calls}. updated {ts} UTC.")


if __name__ == "__main__":
    main()
