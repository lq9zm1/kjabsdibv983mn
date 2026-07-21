#!/usr/bin/env python3
"""
pull_etf_holdings.py — top-N holdings of the benchmark/index ETFs from EODHD -> BigQuery.

Writes table  stonks-498420.stonks_data.etf_top_holdings  (WRITE_TRUNCATE):
    etf STRING, rank INT64, ticker STRING, weight FLOAT64, name STRING, as_of DATE

Purpose: a market-health monitor — the top-15 holdings (by weight) of each benchmark ETF,
refreshed weekly, so you can see how the market's leaders are behaving.

Run (Cloud Shell):
    export EODHD_API_KEY=<your key>
    python3 pull_etf_holdings.py
"""
import os, sys, json, datetime, urllib.request
from google.cloud import bigquery

PROJECT = "stonks-498420"
TABLE   = f"{PROJECT}.stonks_data.etf_top_holdings"
ETFS    = ["QQQ", "SPY", "IWO", "IWM", "RSP", "QQQE"]   # MAGS is a static theme (id357), not here
TOP_N   = 15
KEY     = os.environ.get("EODHD_API_KEY")


def fetch_top(etf):
    """Return the top-N holdings [{ticker,name,weight}] for one ETF, sorted by weight desc."""
    url = f"https://eodhd.com/api/fundamentals/{etf}.US?api_token={KEY}&fmt=json"
    with urllib.request.urlopen(url, timeout=90) as r:
        j = json.load(r)
    etfd = j.get("ETF_Data") or {}
    holdings = etfd.get("Holdings") or {}
    if not holdings:                      # fallback if the full list isn't on the plan
        holdings = etfd.get("Top_10_Holdings") or {}
    out = []
    for k, h in holdings.items():
        h = h or {}
        try:
            w = float(h.get("Assets_%") or 0)
        except (TypeError, ValueError):
            w = 0.0
        code = (h.get("Code") or str(k).split(".")[0]).strip().upper()
        out.append({"ticker": code, "name": (h.get("Name") or "").strip(), "weight": w})
    out.sort(key=lambda x: x["weight"], reverse=True)
    return out[:TOP_N]


def main():
    if not KEY:
        sys.exit("ERROR: export EODHD_API_KEY first")
    today = datetime.date.today().isoformat()
    rows = []
    for etf in ETFS:
        try:
            top = fetch_top(etf)
        except Exception as e:
            print(f"  {etf:5s} FETCH FAILED: {e}", flush=True)
            continue
        if not top:
            print(f"  {etf:5s} no holdings returned (check plan/endpoint)", flush=True)
            continue
        for i, h in enumerate(top, 1):
            rows.append({"etf": etf, "rank": i, "ticker": h["ticker"],
                         "weight": round(h["weight"], 4), "name": h["name"], "as_of": today})
        preview = ", ".join(h["ticker"] for h in top[:6])
        print(f"  {etf:5s} {len(top):2d} holdings  ->  {preview} ...", flush=True)

    if not rows:
        sys.exit("ERROR: nothing fetched — aborting so the table is left unchanged")

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
