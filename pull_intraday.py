"""
pull_intraday.py — INCREMENTAL EODHD intraday puller (Ready-List driven) -> BigQuery price_intraday.
Reads symbols from v_ready_intraday_symbols (Ready tickers + their PARENT ETFs).
For each symbol: pulls 1m bars from MAX(dt_utc) already stored FORWARD (incremental).
New symbol (no history) = last LOOKBACK_DAYS. APPEND (dedupe handled by read-side view v_price_intraday).
Writes a last_updated timestamp to pull_meta (for the sheet freshness cell).

FIRM QUOTA STANDARD: hard cap MAX_API_CALLS; NEVER rely on 500 buffer; respect 1000/min.
SECURITY: API token from env var EODHD_API_KEY — never hardcode.

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
TABLE   = f"{PROJECT}.{DATASET}.price_intraday"
META    = f"{PROJECT}.{DATASET}.pull_meta"
SYM_VIEW = f"{PROJECT}.{DATASET}.v_ready_intraday_symbols"
API_KEY = os.environ.get("EODHD_API_KEY")

INTERVAL = os.environ.get("INTRADAY_INTERVAL", "1m")
LOOKBACK_DAYS = 14          # first pull for a NEW symbol (no history)
OVERLAP_SEC = 120           # re-fetch last 2 min to avoid boundary gaps (dedupe view removes dups)

# ---------- FIRM QUOTA GUARDRAILS ----------
MAX_API_CALLS = 20000
CALLS_PER_REQUEST = 5
SLEEP_SEC = 0.08


def pull_one(symbol, frm, to):
    url = f"https://eodhd.com/api/intraday/{symbol}.US"
    params = {"api_token": API_KEY, "interval": INTERVAL, "fmt": "json", "from": frm, "to": to}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["ticker"] = symbol
    df["interval"] = INTERVAL
    return df


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode the token).")
    client = bigquery.Client(project=PROJECT)

    # symbols to pull (Ready tickers + parent ETFs)
    symbols = [r["symbol"] for r in client.query(
        f"SELECT symbol FROM `{SYM_VIEW}` ORDER BY symbol").result()]
    if not symbols:
        print("No symbols in v_ready_intraday_symbols.")
        return

    # last stored bar per symbol (for this interval)
    last_map = {}
    for r in client.query(
        f"""SELECT ticker, UNIX_SECONDS(MAX(dt_utc)) AS last_ts
            FROM `{TABLE}` WHERE `interval`='{INTERVAL}' GROUP BY ticker""").result():
        last_map[r["ticker"]] = r["last_ts"]

    now = dt.datetime.now(dt.timezone.utc)
    to = int(now.timestamp())
    default_frm = int((now - dt.timedelta(days=LOOKBACK_DAYS)).timestamp())

    calls = 0
    frames = []
    for sym in symbols:
        if calls + CALLS_PER_REQUEST > MAX_API_CALLS:
            print(f"QUOTA CAP reached ({calls}) — aborting before {sym}.")
            break
        frm = (last_map[sym] - OVERLAP_SEC) if sym in last_map else default_frm
        if frm >= to:
            print(f"  {sym}: up to date, skip.")
            continue
        try:
            df = pull_one(sym, frm, to)
            calls += CALLS_PER_REQUEST
            print(f"  {sym}: {len(df)} bars (calls: {calls})")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  !! {sym}: {e}")
        time.sleep(SLEEP_SEC)

    if not frames:
        print("No new data pulled.")
        _write_meta(client, now, 0, calls)
        return

    out = pd.concat(frames, ignore_index=True)
    out["dt_utc"] = pd.to_datetime(out["timestamp"], unit="s", utc=True)
    out["target_date"] = out["dt_utc"].dt.strftime("%Y-%m-%d")
    for c in ("open", "high", "low", "close", "volume"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    keep = ["ticker", "target_date", "interval", "dt_utc", "open", "high", "low", "close", "volume"]
    out = out[keep].dropna(subset=["open", "high", "low", "close"]).drop_duplicates(subset=["ticker", "dt_utc"])

    client.load_table_from_dataframe(
        out, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",   # incremental append; dedupe via v_price_intraday
            time_partitioning=bigquery.TimePartitioning(field="dt_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(out)} bars APPENDED -> price_intraday. API calls: {calls}.")
    _write_meta(client, now, len(out), calls)


def _write_meta(client, now, n_rows, calls):
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    sql = (f"CREATE OR REPLACE TABLE `{META}` AS "
           f"SELECT TIMESTAMP('{ts} UTC') AS last_updated_utc, "
           f"{n_rows} AS rows_appended, {calls} AS api_calls")
    client.query(sql).result()
    print(f"pull_meta updated: {ts} UTC, {n_rows} rows, {calls} calls.")


if __name__ == "__main__":
    main()
