#!/usr/bin/env python3
"""
pull_etf_holdings.py — top-15 holdings of the benchmark ETFs via Alpha Vantage ETF_PROFILE
-> BigQuery stonks-498420.stonks_data.etf_top_holdings (WRITE_TRUNCATE).

Why Alpha Vantage (not the issuer files): the issuer download endpoints (iShares/Invesco)
bot-block datacenter IPs, so they fail from Cloud Shell AND from GitHub-hosted runners.
Alpha Vantage is a real API that serves datacenter IPs → works in the weekly Action too.

Table cols: etf STRING, rank INT64, ticker STRING, weight FLOAT64 (%), name STRING, as_of DATE
Run:  export ALPHAVANTAGE_API_KEY=<free key: https://www.alphavantage.co/support/#api-key>
      python3 pull_etf_holdings.py
Free tier ~25 req/day, ~5/min — 6 ETFs/run is well within it (weekly Action uses 6/week).
"""
import os, sys, json, time, datetime, urllib.request
from google.cloud import bigquery

PROJECT = "stonks-498420"
TABLE   = f"{PROJECT}.stonks_data.etf_top_holdings"
TOP_N   = 15
ETFS    = ["QQQ", "SPY", "IWO", "IWM", "RSP", "QQQE"]
KEY     = os.environ.get("ALPHAVANTAGE_API_KEY")


def fetch(etf):
    url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={etf}&apikey={KEY}"
    with urllib.request.urlopen(url, timeout=60) as r:
        j = json.load(r)
    hold = j.get("holdings")
    if not hold:
        return None, j                       # rate-limit / error → surface raw for diagnostics
    out = []
    for h in hold:
        tk = (h.get("symbol") or "").strip().upper()
        try:
            w = float(h.get("weight") or 0)
        except (TypeError, ValueError):
            w = 0.0
        if not tk or tk in ("N/A", ""):
            continue
        out.append({"ticker": tk, "name": (h.get("description") or "").strip(), "weight": w})
    out.sort(key=lambda x: x["weight"], reverse=True)
    return out[:TOP_N], None


def main():
    if not KEY:
        sys.exit("ERROR: export ALPHAVANTAGE_API_KEY first (free: https://www.alphavantage.co/support/#api-key)")
    today = datetime.date.today().isoformat()
    rows = []
    for i, etf in enumerate(ETFS):
        try:
            top, err = fetch(etf)
        except Exception as e:
            print(f"  {etf:5s} FETCH FAILED: {e}", flush=True)
            continue
        if not top:
            print(f"  {etf:5s} no holdings -> {str(err)[:180]}", flush=True)
            continue
        maxw = max((h["weight"] for h in top), default=0)
        scale = 100.0 if maxw <= 1.5 else 1.0        # AV returns fractions -> percent
        for r, h in enumerate(top, 1):
            rows.append({"etf": etf, "rank": r, "ticker": h["ticker"],
                         "weight": round(h["weight"] * scale, 4), "name": h["name"], "as_of": today})
        print(f"  {etf:5s} {len(top):2d}  ->  {', '.join(h['ticker'] for h in top[:6])} ...", flush=True)
        if i < len(ETFS) - 1:
            time.sleep(13)                            # free tier ~5 req/min

    if not rows:
        sys.exit("ERROR: nothing parsed — check key / daily limit (messages above)")

    schema = [
        bigquery.SchemaField("etf",    "STRING"),
        bigquery.SchemaField("rank",   "INT64"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("weight", "FLOAT64"),
        bigquery.SchemaField("name",   "STRING"),
        bigquery.SchemaField("as_of",  "DATE"),
    ]
    bq = bigquery.Client(project=PROJECT)
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_json(rows, TABLE, job_config=cfg).result()
    print(f"\n✓ Loaded {len(rows)} rows to {TABLE}  (as_of {today})", flush=True)


if __name__ == "__main__":
    main()
