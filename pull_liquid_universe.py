"""
pull_liquid_universe.py — screen the US market for LIQUID names (avg $vol >= $50M)
via the EODHD Screener API, load -> BigQuery `liquid_universe`.

PURPOSE: feed the ticker_review 'candidate' section — liquid names that are NOT in
your curated tickers.txt, so they get surfaced for theming / adding to the universe.

WHY the Screener (not yfinance): yfinance can only pull a KNOWN ticker list; it can't
DISCOVER names outside your universe. The EODHD Screener can.

KEY EODHD CONSTRAINTS (baked into this design):
  • offset caps at 999, limit at 100 -> at most ~1,000 rows PER filter query. A $50M-$vol
    US universe is larger, so we PARTITION by market-cap bands (each < 1,000 rows) and union.
    If a band hits the 1,000 cap we WARN (no silent truncation) so it can be split finer.
  • $vol is NOT a filter field -> we pre-filter on avgvol_200d (shares) + market cap, then
    compute avg_dollar_vol = avgvol_200d * adjusted_close and keep >= $50M in code.
  • each Screener request consumes COST_PER_REQ API-call units (EODHD = 5 today). Parameterized
    below so a change is one edit. A MAX_API_CALLS budget guard aborts cleanly if a runaway
    paging blows past it, and will NOT overwrite liquid_universe with partial data.
  • Screener needs an All-In-One / EOD+Intraday-Extended EODHD plan.

Run via GitHub Action (pull_liquid_universe.yml, weekly) or:
  EODHD_API_KEY=xxx python pull_liquid_universe.py
Requires: requests, pandas, google-cloud-bigquery, db-dtypes
"""
import os
import json
import time
from datetime import date

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TABLE   = f"{PROJECT}.{DATASET}.liquid_universe"
API_KEY = os.environ.get("EODHD_API_KEY")
SCREENER = "https://eodhd.com/api/screener"

MIN_DOLLAR_VOL = 50_000_000        # $50M avg daily dollar volume — THE gate
MIN_SHARE_VOL  = 30_000            # loose pre-filter to trim dead names (safe: a $50M-$vol
                                   # name would need price > $1,666 to fall under this)
US_EXCHANGES   = {"NYSE", "NASDAQ", "NYSE MKT", "AMEX", "NYSE ARCA", "BATS"}
PAGE           = 100               # max limit per request
MAX_OFFSET     = 999               # EODHD hard cap
REQ_PAUSE      = 0.3

# ---- API-cost future-proofing --------------------------------------------------
COST_PER_REQ  = 5        # EODHD: 1 screener request = 5 API-call units. Change here if EODHD changes it.
MAX_API_CALLS = 6000     # hard budget/run (= 1,200 requests). Expected ~a few hundred units; this is a
                         # RUNAWAY RAIL, not the real ceiling (EODHD daily limit is ~100,000).

# market-cap bands (USD). Each should return < 1,000 US names above MIN_SHARE_VOL.
# A $50M/day $vol name almost always has cap >= ~$200M, so we start there.
CAP_BANDS = [
    (200_000_000,     500_000_000),
    (500_000_000,     1_000_000_000),
    (1_000_000_000,   2_000_000_000),
    (2_000_000_000,   5_000_000_000),
    (5_000_000_000,   10_000_000_000),
    (10_000_000_000,  20_000_000_000),
    (20_000_000_000,  50_000_000_000),
    (50_000_000_000,  100_000_000_000),
    (100_000_000_000, 1e15),
]


class _BudgetHit(Exception):
    """Raised when the API-call budget would be exceeded — stops the run cleanly."""


_api_calls = 0


def _screen_request(params):
    """One screener GET, budget-checked. Increments the global cost counter."""
    global _api_calls
    if _api_calls + COST_PER_REQ > MAX_API_CALLS:
        raise _BudgetHit()
    r = requests.get(SCREENER, params=params, timeout=60)
    r.raise_for_status()
    _api_calls += COST_PER_REQ
    return r.json()


def screen_band(lo, hi):
    """Page through one market-cap band; return (rows, hit_cap). May raise _BudgetHit."""
    filters = [["market_capitalization", ">=", lo],
               ["market_capitalization", "<",  hi],
               ["avgvol_200d", ">=", MIN_SHARE_VOL]]
    rows, offset, hit_cap = [], 0, False
    while True:
        data = _screen_request({"api_token": API_KEY, "filters": json.dumps(filters),
                                "sort": "market_capitalization.desc", "limit": PAGE, "offset": offset})
        page = data.get("data", data) if isinstance(data, dict) else data
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
        if offset > MAX_OFFSET:
            hit_cap = True
            break
        time.sleep(REQ_PAUSE)
    return rows, hit_cap


def main():
    if not API_KEY:
        raise SystemExit("Set EODHD_API_KEY env var (do NOT hardcode).")
    client = bigquery.Client(project=PROJECT)

    all_rows, budget_hit = [], False
    try:
        for lo, hi in CAP_BANDS:
            band, hit_cap = screen_band(lo, hi)
            tag = "  !! HIT 1,000-ROW CAP — SPLIT THIS BAND FINER" if hit_cap else ""
            print(f"band [{lo:,.0f}-{hi:,.0f}]: {len(band)} rows{tag}")
            all_rows.extend(band)
            time.sleep(REQ_PAUSE)
    except _BudgetHit:
        budget_hit = True
        print(f"  !! API BUDGET HIT ({_api_calls} of {MAX_API_CALLS} call-units) — stopping early.")

    print(f"screener cost this run: ~{_api_calls} call-units ({_api_calls // COST_PER_REQ} requests).")

    if budget_hit:
        print("   PARTIAL data — liquid_universe left UNCHANGED (last-good preserved). "
              "Raise MAX_API_CALLS and re-run.")
        return
    if not all_rows:
        print("No rows from screener — aborting (liquid_universe NOT overwritten).")
        return

    df = pd.DataFrame(all_rows)
    print(f"raw screener rows: {len(df)}", flush=True)
    if len(df):
        print("  columns:", list(df.columns), flush=True)
        print("  sample:", {k: df.iloc[0].get(k) for k in
              ("code", "name", "exchange", "market_capitalization", "avgvol_200d", "adjusted_close")}, flush=True)
        if "exchange" in df:
            print("  exchanges:", df["exchange"].astype(str).value_counts().head(15).to_dict(), flush=True)
    if "avgvol_200d" not in df.columns or "adjusted_close" not in df.columns:
        print("!! screener response missing avgvol_200d / adjusted_close — see 'columns' above; aborting.")
        return
    for c in ("market_capitalization", "avgvol_200d", "adjusted_close", "avgvol_1d"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["avg_dollar_vol"] = df["avgvol_200d"] * df["adjusted_close"]

    # gates — the endpoint is already /US, so only EXCLUDE OTC/pink (don't whitelist exchanges):
    OTC = {"OTC", "OTCQB", "OTCQX", "OTCMKTS", "PINK", "GREY", "EXPM"}
    if "exchange" in df.columns:
        df = df[~df["exchange"].astype(str).str.upper().isin(OTC)]
    print(f"after exchange filter: {len(df)}", flush=True)
    df = df[df["avg_dollar_vol"] >= MIN_DOLLAR_VOL]
    print(f"after >= ${MIN_DOLLAR_VOL/1e6:.0f}M avg $vol: {len(df)}", flush=True)
    df = df[~df["code"].astype(str).str.upper().str.contains(r"[.\-](WS|WT|U|RT|R)$", na=False, regex=True)]
    df = df.dropna(subset=["code"]).drop_duplicates(subset=["code"])
    print(f"after warrant/dedup: {len(df)}", flush=True)

    out = pd.DataFrame({
        "ticker": df["code"].astype(str).str.upper().str.strip(),
        "name": df.get("name"),
        "exchange": df.get("exchange"),
        "sector": df.get("sector"),
        "industry": df.get("industry"),
        "market_cap": df["market_capitalization"],
        "avgvol_200d": df["avgvol_200d"],
        "close": df["adjusted_close"],
        "avg_dollar_vol": df["avg_dollar_vol"].round(0),
        "asof_date": date.today(),
    })
    out["asof_date"] = pd.to_datetime(out["asof_date"]).dt.date

    client.load_table_from_dataframe(
        out, TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()
    print(f"Done: {len(out):,} liquid names (>= ${MIN_DOLLAR_VOL/1e6:.0f}M avg $vol) -> liquid_universe.")


if __name__ == "__main__":
    main()
