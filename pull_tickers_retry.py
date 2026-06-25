"""
pull_tickers_retry.py — recover the yfinance .info rows that errored.

Reads tickers_raw, selects ONLY rows where error IS NOT NULL, re-pulls them
with throttling + exponential backoff, and MERGES the recovered rows back into
tickers_raw (updates those tickers; leaves already-good rows untouched).

Idempotent + re-runnable: each pass shrinks the error set. Run 2-3x across a day.
Same auth as pull.py (GCP_SA_KEY -> key.json), triggered from the Actions tab.
"""

import time
import random
import pandas as pd
import yfinance as yf
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE   = "tickers_raw"

# throttle knobs — gentle to dodge Yahoo rate-limiting from datacenter IPs
BASE_SLEEP   = 1.5     # seconds between requests
MAX_RETRIES  = 4       # per-ticker retry attempts
BATCH_REPORT = 25      # progress print cadence

FIELDS = [
    "longName", "quoteType", "sector", "industry",
    "country", "currency", "exchange",
    "dividendYield", "marketCap",
]


def fetch(sym):
    """Fetch one ticker with backoff. Returns row dict; error=None on success."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            info = yf.Ticker(sym).info or {}
            # a real success returns a populated dict; empty = treat as soft fail
            if not info or info.get("quoteType") is None and info.get("longName") is None:
                raise ValueError("empty .info")
            row = {"ticker": sym, "error": None}
            for k in FIELDS:
                row[k] = info.get(k)
            return row
        except Exception as e:
            last_err = str(e)[:300]
            # exponential backoff with jitter
            time.sleep(BASE_SLEEP * (2 ** attempt) + random.uniform(0, 1))
    row = {"ticker": sym, "error": last_err}
    for k in FIELDS:
        row[k] = None
    return row


def main():
    client = bigquery.Client(project=PROJECT)

    # 1) which tickers still need recovery?
    todo = [r.ticker for r in client.query(
        f"SELECT ticker FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE error IS NOT NULL"
    ).result()]
    print(f"errored tickers to retry: {len(todo)}")
    if not todo:
        print("nothing to retry — all clean.")
        return

    # 2) re-pull with throttle
    rows = []
    for i, s in enumerate(todo, 1):
        rows.append(fetch(s))
        time.sleep(BASE_SLEEP)
        if i % BATCH_REPORT == 0:
            ok = sum(1 for r in rows if r["error"] is None)
            print(f"  {i}/{len(todo)}  recovered so far: {ok}")

    df = pd.DataFrame(rows)
    for c in ["longName", "quoteType", "sector", "industry",
              "country", "currency", "exchange", "error"]:
        df[c] = df[c].astype("string")
    df["dividendYield"] = pd.to_numeric(df["dividendYield"], errors="coerce")
    df["marketCap"]     = pd.to_numeric(df["marketCap"], errors="coerce")

    recovered = int(df["error"].isna().sum())
    print(f"this pass recovered {recovered}/{len(df)}")

    # 3) stage to a temp table, then MERGE (update matched rows only)
    staging = f"{PROJECT}.{DATASET}._tickers_retry_stage"
    client.load_table_from_dataframe(
        df, staging,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    merge_sql = f"""
    MERGE `{PROJECT}.{DATASET}.{TABLE}` T
    USING `{staging}` S
    ON T.ticker = S.ticker
    WHEN MATCHED THEN UPDATE SET
      longName=S.longName, quoteType=S.quoteType, sector=S.sector,
      industry=S.industry, country=S.country, currency=S.currency,
      exchange=S.exchange, dividendYield=S.dividendYield,
      marketCap=S.marketCap, error=S.error
    """
    client.query(merge_sql).result()
    client.query(f"DROP TABLE `{staging}`").result()

    # 4) report remaining
    remaining = list(client.query(
        f"SELECT COUNT(*) AS n FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE error IS NOT NULL"
    ).result())[0].n
    print(f"MERGE done. errored remaining in {TABLE}: {remaining}")


if __name__ == "__main__":
    main()
