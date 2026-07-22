#!/usr/bin/env python3
"""
run_nightly.py — Stonks nightly ingestion + rebuild orchestrator.
Order:
  1. Directory sync  (NASDAQ Trader = source of truth for valid/active tickers)
  2. Pull prices     (yfinance, only for curated tickers still in the directory)
  3. Load price_history  (WRITE_TRUNCATE, partitioned by date / clustered by ticker)
  4. Run sql/*.sql in filename order  (metrics_daily -> ... -> theme_rs_history)
  5. Refresh tickers metadata  (new tickers only; keep last-known on failure)
  6. Write ticker_review table  (Delisted / No-data-streak / New listings)
The curated universe (tickers.txt) is NEVER auto-edited, and stock_theme_map is
NEVER auto-touched. Confirmed-delisted are only skipped from the pull and surfaced
in ticker_review for you to act on.
"""
import os
import re
import sys
import io
import time
from datetime import date
from pathlib import Path
import pandas as pd
import requests
import yfinance as yf
from google.cloud import bigquery
# ---- config -----------------------------------------------------------------
PROJECT = "stonks-498420"
DATASET = "stonks_data"
TICKERS_FILE = "tickers.txt"
SQL_DIR = "sql"
DELIST_CONFIRM_NIGHTS = 3     # absent from directory this many nights -> confirmed
NODATA_STREAK_NIGHTS  = 3     # yfinance failing this many nights -> flag
NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
import pull as pullmod
bq = bigquery.Client(project=PROJECT)
TODAY = date.today()
def tbl(name): return f"{PROJECT}.{DATASET}.{name}"
def norm(s):   return str(s).upper().strip().replace(".", "-").replace("/", "-")
# ---- 0. helpers -------------------------------------------------------------
def load_curated():
    raw = Path(TICKERS_FILE).read_text()
    toks = re.split(r"[^A-Za-z0-9.\-]+", raw.upper())
    seen, out = set(), []
    for t in toks:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out
def run_query(sql):
    return bq.query(sql).result()
def table_exists(name):
    try:
        bq.get_table(tbl(name)); return True
    except Exception:
        return False
# ---- 1. directory sync ------------------------------------------------------
def fetch_directory():
    active = set()
    for url, sym_col, test_col in [
        (NASDAQ_LISTED, "Symbol", "Test Issue"),
        (OTHER_LISTED, "ACT Symbol", "Test Issue"),
    ]:
        txt = requests.get(url, timeout=60).text
        df = pd.read_csv(io.StringIO(txt), sep="|", dtype=str)
        # last row is a "File Creation Time" footer -> drop rows with no symbol
        df = df[df[sym_col].notna() & ~df[sym_col].str.contains("File Creation", na=False)]
        if test_col in df.columns:
            df = df[df[test_col].fillna("N").str.upper() != "Y"]
        active |= {norm(s) for s in df[sym_col].tolist()}
    return active
def directory_sync(curated):
    print("1. directory sync ...", flush=True)
    active = fetch_directory()
    print(f"   directory active symbols: {len(active):,}")
    # snapshot today's directory for new-listing diffs
    snap = pd.DataFrame({"date": [TODAY] * len(active), "symbol": sorted(active)})
    snap["date"] = pd.to_datetime(snap["date"]).dt.date
    bq.load_table_from_dataframe(
        snap, tbl("directory_symbols"),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    # new listings = today's directory minus most recent prior snapshot
    new_syms = []
    if table_exists("directory_symbols"):
        q = f"""SELECT symbol FROM `{tbl('directory_symbols')}`
                WHERE date = (SELECT MAX(date) FROM `{tbl('directory_symbols')}` WHERE date < '{TODAY}')"""
        try:
            prior = {r.symbol for r in bq.query(q).result()}
            if prior:
                new_syms = sorted(active - prior)
        except Exception as e:
            print("   (new-listing diff skipped:", e, ")")
    # which curated tickers are absent from the directory today
    absent = [t for t in curated if norm(t) not in active]
    pull_list = [t for t in curated if norm(t) in active]
    if "SPY" not in pull_list:
        pull_list.append("SPY")   # benchmark must always be pulled
    print(f"   curated kept (in directory): {len(pull_list):,}  |  absent: {len(absent)}")
    return pull_list, absent, new_syms
# ---- 2-3. pull + load -------------------------------------------------------
def pull_and_load(pull_list):
    print("2. pulling prices ...", flush=True)
    df, failed = pullmod.pull_prices(pull_list)
    print(f"   pulled {len(df):,} rows for {df['ticker'].nunique()} tickers  | failed: {len(failed)}")
    if df.empty:
        raise RuntimeError("pull returned no data - aborting (price_history NOT overwritten)")
    print("3. loading price_history (truncate) ...", flush=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("adj_close", "FLOAT64"),
        bigquery.SchemaField("volume", "INT64"),
    ]
    cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        # CHANGED: month-grain partitioning. Day-grain caps at 4,000 partitions per
        # load (and 10,000 per table); "max" history spans >4,000 trading days and
        # blew past it. MONTH = ~550 partitions for 45yr. Same daily rows, same
        # weekly resample — only the on-disk partition granularity changes.
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date",
        ),
        clustering_fields=["ticker"],
    )
    bq.load_table_from_dataframe(df, tbl("price_history"), job_config=cfg).result()
    return failed
# ---- 4. rebuilds  (timed per SQL file) --------------------------------------
def run_rebuilds():
    files = sorted(Path(SQL_DIR).glob("*.sql"))
    print(f"4. running {len(files)} rebuilds ...", flush=True)
    timings = []
    for f in files:
        t0 = time.perf_counter()
        job = bq.query(f.read_text())
        job.result()                      # wait for completion
        dt = time.perf_counter() - t0
        gb = (job.total_bytes_processed or 0) / 1e9
        slot_min = (job.slot_millis or 0) / 60000.0
        timings.append((f.name, dt, gb, slot_min))
        print(f"   -> {f.name:34} {dt:7.1f}s  {gb:8.2f} GB  {slot_min:7.1f} slot-min", flush=True)

    print("\n   -- SQL REBUILD TIMINGS (slowest first) --", flush=True)
    print(f"   {'seconds':>8}  {'GB scan':>9}  {'slot-min':>9}  file", flush=True)
    for name, dt, gb, sm in sorted(timings, key=lambda x: -x[1]):
        print(f"   {dt:8.1f}  {gb:9.2f}  {sm:9.1f}  {name}", flush=True)
    print(f"   {sum(t[1] for t in timings):8.1f}  {'':9}  {'':9}  TOTAL sql rebuilds", flush=True)
# ---- 5. tickers metadata (new tickers only; keep last-known) ----------------
def refresh_tickers_metadata(pull_list):
    N_PER_NIGHT = 300   # cap so the job stays under timeout; blanks fill over a few nights
    print("5. refreshing tickers metadata (new + blanks, capped) ...", flush=True)
    existing = pd.DataFrame(columns=["ticker", "name", "sector", "industry"])
    if table_exists("tickers"):
        existing = bq.query(
            f"SELECT ticker,name,sector,industry FROM `{tbl('tickers')}`"
        ).to_dataframe()
    active = set(pull_list)
    meta = {}
    if not existing.empty:
        for _, r in existing.iterrows():
            if r["ticker"] in active:
                meta[r["ticker"]] = {"name": r.get("name"),
                                     "sector": r.get("sector"),
                                     "industry": r.get("industry")}
    for t in pull_list:                       # ensure every active ticker has a row
        meta.setdefault(t, {"name": None, "sector": None, "industry": None})
    def _blank(v):
        return v is None or (isinstance(v, float) and pd.isna(v)) or v == ""
    def is_blank(m):
        return _blank(m["name"]) or _blank(m["sector"]) or _blank(m["industry"])
    targets = [t for t in pull_list if is_blank(meta[t])][:N_PER_NIGHT]
    print(f"   {len(targets)} tickers need metadata this run (cap {N_PER_NIGHT})")
    filled = 0
    for t in targets:
        try:
            info = yf.Ticker(pullmod.to_yahoo(t)).get_info()
            nm  = info.get("longName") or info.get("shortName")
            sec = info.get("sector"); ind = info.get("industry")
            if nm:  meta[t]["name"] = nm        # only overwrite when we got a value
            if sec: meta[t]["sector"] = sec
            if ind: meta[t]["industry"] = ind
            if nm or sec or ind: filled += 1
        except Exception:
            pass
    out = pd.DataFrame([{"ticker": t, **m} for t, m in meta.items()])
    if out.empty:
        print("   (no tickers metadata to write)"); return
    bq.load_table_from_dataframe(
        out, tbl("tickers"),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()
    remaining = sum(1 for t in pull_list if is_blank(meta[t]))
    print(f"   tickers table: {len(out):,} rows | filled {filled} this run | {remaining} still blank")
# ---- 6. review log + ticker_review table ------------------------------------
def write_review(curated, pull_list, absent, failed, new_syms):
    print("6. writing ticker_review ...", flush=True)
    active_set = set(pull_list)
    failed_set = set(failed)
    log = pd.DataFrame({
        "date": TODAY,
        "ticker": curated,
        "in_directory": [t in active_set for t in curated],
        "pull_ok": [(t in active_set) and (t not in failed_set) for t in curated],
    })
    log["date"] = pd.to_datetime(log["date"]).dt.date
    bq.load_table_from_dataframe(
        log, tbl("ticker_review_log"),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    # themes per ticker
    themes = {}
    try:
        tq = bq.query(f"""SELECT ticker, STRING_AGG(sub_theme, ', ' ORDER BY sub_theme) themes
                          FROM `{tbl('stock_theme_map')}` GROUP BY ticker""").result()
        themes = {r.ticker: r.themes for r in tq}
    except Exception:
        pass
    names = {}
    if table_exists("tickers"):
        try:
            nq = bq.query(f"SELECT ticker,name FROM `{tbl('tickers')}`").result()
            names = {r.ticker: r.name for r in nq}
        except Exception:
            pass
    # streak helpers from the log
    streak = bq.query(f"""
        WITH recent AS (
          SELECT ticker, date, in_directory, pull_ok,
                 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) rn
          FROM `{tbl('ticker_review_log')}`
        )
        SELECT ticker,
          SUM(CASE WHEN rn<={DELIST_CONFIRM_NIGHTS} AND NOT in_directory THEN 1 ELSE 0 END) AS dir_absent,
          SUM(CASE WHEN rn<={NODATA_STREAK_NIGHTS} AND in_directory AND NOT pull_ok THEN 1 ELSE 0 END) AS nodata
        FROM recent GROUP BY ticker
    """).result()
    absent_streak = {r.ticker: r.dir_absent for r in streak}
    nodata_streak = {}
    for r in bq.query(f"""
        WITH recent AS (
          SELECT ticker, in_directory, pull_ok,
                 ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) rn
          FROM `{tbl('ticker_review_log')}`)
        SELECT ticker,
          SUM(CASE WHEN rn<={NODATA_STREAK_NIGHTS} AND in_directory AND NOT pull_ok THEN 1 ELSE 0 END) nd
        FROM recent GROUP BY ticker""").result():
        nodata_streak[r.ticker] = r.nd
    review_rows = []
    # delisted (confirmed = absent the full window)
    for t in absent:
        n = absent_streak.get(t, 1)
        review_rows.append({
            "section": "delisted", "ticker": t,
            "name": names.get(t), "themes": themes.get(t),
            "detail": f"absent from directory {n} night(s)"
                      + (" — CONFIRMED" if n >= DELIST_CONFIRM_NIGHTS else " — confirming"),
            "as_of": TODAY,
        })
    # no-data streak (still listed, yfinance failing)
    for t, n in nodata_streak.items():
        if n >= NODATA_STREAK_NIGHTS:
            review_rows.append({
                "section": "no_data", "ticker": t,
                "name": names.get(t), "themes": themes.get(t),
                "detail": f"no price data {n} night(s) (still listed)",
                "as_of": TODAY,
            })
    # new listings (directory diff)
    for s in new_syms:
        review_rows.append({
            "section": "new", "ticker": s, "name": names.get(s),
            "themes": None, "detail": "new in directory — theme it if relevant",
            "as_of": TODAY,
        })
    rev = pd.DataFrame(review_rows, columns=["section", "ticker", "name", "themes", "detail", "as_of"])
    if rev.empty:
        rev = pd.DataFrame([{"section": "ok", "ticker": "-", "name": None,
                             "themes": None, "detail": "no changes tonight", "as_of": TODAY}])
    rev["as_of"] = pd.to_datetime(rev["as_of"]).dt.date
    bq.load_table_from_dataframe(
        rev, tbl("ticker_review"),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()
    print(f"   review: {len(absent)} delisted, "
          f"{sum(1 for n in nodata_streak.values() if n>=NODATA_STREAK_NIGHTS)} no-data, "
          f"{len(new_syms)} new")
def main():
    T = {}
    def clk(): return time.perf_counter()

    curated = load_curated()
    print(f"curated universe: {len(curated):,} tickers")

    t = clk(); pull_list, absent, new_syms = directory_sync(curated);   T['1 directory_sync'] = clk() - t
    t = clk(); failed = pull_and_load(pull_list);                       T['2-3 pull + load']  = clk() - t
    t = clk(); run_rebuilds();                                          T['4 sql rebuilds']   = clk() - t

    t = clk()
    try:
        refresh_tickers_metadata(pull_list)
    except Exception as e:
        print("   (metadata refresh skipped:", e, ")")
    T['5 tickers metadata'] = clk() - t

    t = clk()
    try:
        write_review(curated, pull_list, absent, failed, new_syms)
    except Exception as e:
        print("   (review step error:", e, ")")
    T['6 ticker_review'] = clk() - t

    print("\n== PHASE TIMINGS (slowest first) ==", flush=True)
    for k, v in sorted(T.items(), key=lambda x: -x[1]):
        print(f"  {v:8.1f}s  {v/60:5.1f}m  {k}", flush=True)
    print(f"  {sum(T.values()):8.1f}s  {sum(T.values())/60:5.1f}m  TOTAL", flush=True)
    print("DONE.", flush=True)
if __name__ == "__main__":
    main()
