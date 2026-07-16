"""
pull_etfs_v2.py — backfill ETF EOD daily history -> BigQuery `etf_prices`.
v2 change (2026-07-16): universe now comes from the dedicated `etf_universe` table
(source='eodhd' only — the US ETFs; tv-source macros like ^VIX/futures are backtest-only via TV).
Also bumped MAX_API_CALLS 100 -> 250 (eodhd universe is now ~145 ETFs; old cap would abort mid-run).

Run via GitHub Action (pull_etfs.yml) or: EODHD_API_KEY=xxx python pull_etfs_v2.py
Requires: requests, pandas, google-cloud-bigquery, db-dtypes
"""
import os
import time

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE = f"{PROJECT}.{DATASET}.etf_prices"
API_KEY = os.environ.get("EODHD_API_KEY")
FROM_DATE = "1990-01-01"     # grab full inception history (EODHD returns each ETF from its start;
                             # 1 call/ticker regardless of range → no extra API cost). Was 2019-01-01.
MAX_API_CALLS = 250          # was 100 — universe grew to ~145 ETFs; keep headroom


def pull_eod(ticker):
    url = f"https://eodhd.com/api/eod/{ticker}.US"
    params = {"api_token": API_KEY, "from": FROM_DATE, "period": "d", "fmt": "json"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).rename(columns={"adjusted_close": "adj_close"})
    df["ticker"] = ticker
    return df


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode).")
    client = bigquery.Client(project=PROJECT)
    # v2: pull the ETF universe from the dedicated table, EODHD-pullable US ETFs only
    etfs = [r.ticker for r in client.query(
        f"SELECT DISTINCT ticker FROM `{PROJECT}.{DATASET}.etf_universe` "
        f"WHERE source = 'eodhd' AND ticker IS NOT NULL"
    ).result()]
    if "SPY" not in etfs:
        etfs.append("SPY")
    print(f"{len(etfs)} ETFs (+SPY) to pull.")

    frames, calls = [], 0
    for tk in etfs:
        if calls >= MAX_API_CALLS:
            print("API cap hit — aborting.")
            break
        try:
            df = pull_eod(tk)
            calls += 1
            print(f"  {tk}: {len(df)} bars  (calls={calls})")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  !! {tk}: {e}")
        time.sleep(0.1)

    if not frames:
        print("No data pulled.")
        return
    out = pd.concat(frames, ignore_index=True)
    for c in ("open", "high", "low", "close", "adj_close", "volume"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["date"] = pd.to_datetime(out["date"])
    out = out[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]].dropna(subset=["close"])

    client.load_table_from_dataframe(
        out, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(field="date", type_="MONTH"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(out):,} rows, {out['ticker'].nunique()} ETFs -> etf_prices.  API calls: {calls}.")


if __name__ == "__main__":
    main()
