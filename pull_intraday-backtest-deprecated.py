"""
pull_intraday.py — TARGETED EODHD intraday puller (quota-guarded) -> BigQuery price_intraday.
Pulls a +/-WINDOW_DAYS window of 5m bars around each (ticker, entry_date) target.

🚨 FIRM QUOTA STANDARD (REV-C39): hard cap MAX_API_CALLS; NEVER rely on the 500 buffer;
respect 1000/min; log calls; ABORT before exceeding the cap.
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

INTERVAL = os.environ.get("INTRADAY_INTERVAL", "5m")   # 5m (600d) or 1m (120d, for 2m/3m resample)
WINDOW_DAYS_BEFORE = 25  # prior days pulled (for TIME-OF-DAY RVOL baseline; ~17 trading days)
WINDOW_DAYS_AFTER = 5    # days after the entry day

# ---------- FIRM QUOTA GUARDRAILS ----------
MAX_API_CALLS = 20000    # hard cap (~20% of the 100k/day plan). NEVER touch the 500 buffer.
CALLS_PER_REQUEST = 5    # EODHD intraday = 5 API calls per request
SLEEP_SEC = 0.08         # ~750 req/min, safely under the 1000/min limit

# ---------- TARGETS: (ticker, 'YYYY-MM-DD') entry days. Edit for the BASIC TEST. ----------
# For the basic test, put a few Flat Base / Pocket Pivot entry days here (recent = also 1m-capable).
# Cyber-EDR group + parent ETF CIBR, on 3 flat-base entry days (2 grade-A + a recent B pair).
# All 5 tickers each day -> so we can rank the group's intraday RVOL/RS leader vs the ETF.
TARGETS = [
    ("CRWD","2025-11-10"),("PANW","2025-11-10"),("DDOG","2025-11-10"),("FTNT","2025-11-10"),("CIBR","2025-11-10"),
    ("CRWD","2025-11-06"),("PANW","2025-11-06"),("DDOG","2025-11-06"),("FTNT","2025-11-06"),("CIBR","2025-11-06"),
    ("CRWD","2026-06-26"),("PANW","2026-06-26"),("DDOG","2026-06-26"),("FTNT","2026-06-26"),("CIBR","2026-06-26"),
]


def to_unix(d):
    return int(dt.datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())


def pull_one(ticker, entry_date):
    frm = to_unix(entry_date) - WINDOW_DAYS_BEFORE * 86400
    to = to_unix(entry_date) + (WINDOW_DAYS_AFTER + 1) * 86400
    url = f"https://eodhd.com/api/intraday/{ticker}.US"
    params = {"api_token": API_KEY, "interval": INTERVAL, "fmt": "json", "from": frm, "to": to}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["ticker"] = ticker
    df["target_date"] = entry_date
    df["interval"] = INTERVAL
    return df


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode the token).")
    client = bigquery.Client(project=PROJECT)
    calls = 0
    frames = []
    for i, (tk, d) in enumerate(TARGETS):
        if calls + CALLS_PER_REQUEST > MAX_API_CALLS:
            print(f"QUOTA CAP reached ({calls} calls) — aborting before target {i} ({tk} {d}).")
            break
        try:
            df = pull_one(tk, d)
            calls += CALLS_PER_REQUEST
            print(f"  {tk} {d}: {len(df)} bars  (calls so far: {calls})")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  !! {tk} {d}: {e}")
        time.sleep(SLEEP_SEC)

    if not frames:
        print("No data pulled.")
        return

    out = pd.concat(frames, ignore_index=True)
    # EODHD intraday fields: timestamp(unix), gmtoffset, datetime(str UTC), open, high, low, close, volume
    out["dt_utc"] = pd.to_datetime(out["timestamp"], unit="s", utc=True)
    for c in ("open", "high", "low", "close", "volume"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    keep = ["ticker", "target_date", "interval", "dt_utc", "open", "high", "low", "close", "volume"]
    out = out[keep].dropna(subset=["open", "high", "low", "close"])

    client.load_table_from_dataframe(
        out, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(field="dt_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(out)} intraday bars -> price_intraday. API calls used: {calls} (cap {MAX_API_CALLS}).")
    print("NOTE: WRITE_APPEND — re-running duplicates rows; dedupe on (ticker,dt_utc) later for production.")


if __name__ == "__main__":
    main()
