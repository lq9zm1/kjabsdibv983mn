#!/usr/bin/env python3
"""
pull_spx.py  —  nightly EODHD S&P 500 cash-index (GSPC.INDX) appender for spx_daily.

WHY: the daily SATA Mansfield band (and the weekly Mansfield's current week) read
spx_daily. It was a one-time TradingView TVC:SPX load and goes stale. This appends
the latest trading days from EODHD's GSPC.INDX (the same ^GSPC cash index) so the
latest bar is live.

WHAT IT DOES:
  - Reads MAX(date) currently in spx_daily.
  - Fetches GSPC.INDX daily bars from MAX(date)+1 -> today via the same EODHD EOD
    endpoint pull.py uses.
  - APPENDS only strictly-new dates, in spx_daily's existing schema
    (time INT64 = unix seconds @ 13:30 UTC to match the TV bars; open/high/low/close
    FLOAT64). Historical TV rows are never touched -> zero re-validation, and the
    single-vendor "seam" at the frontier washes out of the 52-bar Mansfield MA
    (same cash index, ~0% level diff).
  - Mansfield only uses `close`; OHLC are stored for schema completeness (fall back
    to close if EODHD omits them for the index).

RUN ORDER: must run BEFORE run_nightly.py so the SQL rebuilds (11_sata_score,
14_sata_daily, band_05_mansfield*) see fresh SPX.

REQUIRES: requests, google-cloud-bigquery (transitive via pandas-gbq).
ENV: EODHD_API_KEY ; GOOGLE_APPLICATION_CREDENTIALS (BQ service-account).
"""

import os
import sys
import time
import datetime as dt

import requests
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE   = f"{PROJECT}.{DATASET}.spx_daily"
SYMBOL  = "GSPC.INDX"
BASE    = "https://eodhd.com/api/eod"
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")
TIMEOUT = 60
RETRY   = 2

SCHEMA = [
    bigquery.SchemaField("time",  "INT64"),
    bigquery.SchemaField("open",  "FLOAT64"),
    bigquery.SchemaField("high",  "FLOAT64"),
    bigquery.SchemaField("low",   "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
]


def latest_date(client):
    """MAX trading date currently in spx_daily (datetime.date), or None if empty."""
    sql = f"SELECT MAX(DATE(TIMESTAMP_SECONDS(`time`))) AS d FROM `{TABLE}`"
    return list(client.query(sql).result())[0]["d"]


def fetch_gspc(start_date):
    """Fetch GSPC.INDX daily bars from start_date (inclusive) to today."""
    url = f"{BASE}/{SYMBOL}"
    params = {"api_token": EODHD_API_KEY, "from": start_date.isoformat(),
              "period": "d", "fmt": "json"}
    for attempt in range(RETRY + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
                print(f"WARNING: unexpected EODHD response (not a list): {str(data)[:200]}",
                      flush=True)
                return []
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError("GSPC.INDX fetch failed after retries")


def to_unix_seconds(date_str):
    """'YYYY-MM-DD' -> unix seconds @ 13:30 UTC (matches the TV daily-bar convention;
    only DATE() is used downstream, so the time-of-day is cosmetic but kept uniform)."""
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=13, minute=30, tzinfo=dt.timezone.utc)
    return int(d.timestamp())


def main():
    if not EODHD_API_KEY:
        print("ERROR: EODHD_API_KEY not set.", flush=True)
        sys.exit(1)

    client = bigquery.Client(project=PROJECT)
    last = latest_date(client)
    if last is None:
        print("ERROR: spx_daily is empty (expected the historical TV load).", flush=True)
        sys.exit(1)

    start = last + dt.timedelta(days=1)
    today = dt.date.today()
    if start > today:
        print(f"spx_daily already current through {last}. Nothing to append.", flush=True)
        return

    print(f"spx_daily max date = {last}. Fetching GSPC.INDX from {start} ...", flush=True)
    bars = fetch_gspc(start)

    rows = []
    for b in bars:
        try:
            d = b["date"]
            if dt.datetime.strptime(d, "%Y-%m-%d").date() <= last:
                continue                      # strictly-new dates only
            c = float(b["close"])
            rows.append({
                "time":  to_unix_seconds(d),
                "open":  float(b.get("open")  or c),
                "high":  float(b.get("high")  or c),
                "low":   float(b.get("low")   or c),
                "close": c,
            })
        except (KeyError, TypeError, ValueError):
            continue

    if not rows:
        print("No new trading days returned (market closed / already current).", flush=True)
        return

    job = client.load_table_from_json(
        rows, TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=SCHEMA),
    )
    job.result()
    newest = dt.datetime.fromtimestamp(max(r["time"] for r in rows), dt.timezone.utc).date()
    print(f"Appended {len(rows)} rows to spx_daily (now current through {newest}).", flush=True)


if __name__ == "__main__":
    main()
