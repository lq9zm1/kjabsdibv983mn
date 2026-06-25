"""
pull_tickers.py — build the `tickers` dimension (metadata, not prices).

Fetches yfinance .info per symbol (sector, industry, country, dividend yield,
quote type) and loads it to BigQuery table `stonks_data.tickers_raw`.

Runs in GitHub Actions (same auth as pull.py: GCP_SA_KEY -> key.json).
This is SLOW + rate-limited (~1 request/ticker), so it is a manual/weekly job,
NOT part of the nightly. Classification (is_adr, accuracy_tier, ...) is done in
a SQL view on top of tickers_raw, so thresholds can be tuned without re-pulling.

USAGE: flip TEST_MODE to switch between the 10-name validation list and the
full universe in tickers.txt.
"""

import time
import pandas as pd
import yfinance as yf
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE   = "tickers_raw"

# ── TEST_MODE: start True (10 names, ~1 min) to validate fields.
#    Flip to False for the full universe from tickers.txt.
TEST_MODE  = False
TEST_LIST  = ["BBVA", "NVO", "ASML", "KMB", "XOM", "HOOD", "MSFT", "V", "TSLA", "AMZN"]

FIELDS = [
    "longName", "quoteType", "sector", "industry",
    "country", "currency", "exchange",
    "dividendYield", "marketCap",
]


def load_universe():
    if TEST_MODE:
        return TEST_LIST
    with open("tickers.txt") as f:
        return [ln.strip() for ln in f if ln.strip()]


def fetch(sym):
    row = {"ticker": sym}
    try:
        info = yf.Ticker(sym).info or {}
        for k in FIELDS:
            row[k] = info.get(k)
        row["error"] = None
    except Exception as e:
        for k in FIELDS:
            row[k] = None
        row["error"] = str(e)[:300]
    return row


def main():
    syms = load_universe()
    print(f"TEST_MODE={TEST_MODE} — fetching {len(syms)} tickers")
    rows = []
    for i, s in enumerate(syms, 1):
        rows.append(fetch(s))
        if i % 50 == 0:
            print(f"  {i}/{len(syms)}")
            time.sleep(1)          # be gentle on the API for large pulls
    df = pd.DataFrame(rows)

    # normalize types so BQ schema is stable
    for c in ["longName", "quoteType", "sector", "industry",
              "country", "currency", "exchange", "error"]:
        df[c] = df[c].astype("string")
    df["dividendYield"] = pd.to_numeric(df["dividendYield"], errors="coerce")
    df["marketCap"]     = pd.to_numeric(df["marketCap"], errors="coerce")

    client = bigquery.Client(project=PROJECT)
    client.load_table_from_dataframe(
        df, f"{PROJECT}.{DATASET}.{TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    ok = df["error"].isna().sum()
    print(f"loaded {len(df)} rows to {DATASET}.{TABLE}  ({ok} ok, {len(df)-ok} errored)")


if __name__ == "__main__":
    main()
