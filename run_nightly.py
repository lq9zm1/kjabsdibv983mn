#!/usr/bin/env python3
"""
run_nightly.py — Stonks nightly ingestion + rebuild orchestrator.

Order:
  0. Apply ticker_review Submits  (apply_review_actions: queued theme-adds / removes -> tickers.txt
                                   + theme_map, BEFORE the universe is read, so they take effect tonight)
  1. Directory sync  (NASDAQ Trader = source of truth for valid/active tickers)
  2. Pull prices     (EODHD; Sunday = FULL history reset, weekdays = INCREMENTAL last-7d upsert)
  3. Load price_history  (Sunday: WRITE_TRUNCATE; weekdays: MERGE-upsert the recent window)
  4. Run sql/*.sql in filename order  (metrics_daily -> ... -> theme_rs_history), timed per file
  5. Refresh tickers metadata  (new tickers only; keep last-known on failure)
  6. Write ticker_review table  (Delisted / No-data-streak / New listings / Candidates)

tickers.txt / theme_map are edited ONLY by step 0's apply_review_actions module (which applies YOUR
Submits and commits tickers.txt back) — the rebuild logic in THIS file never mutates them. Confirmed-
delisted names you haven't actioned are skipped from the pull and surfaced in ticker_review; the
separate weekly prune-delisted Action auto-cleans the ones confirmed >=3 nights.
"""
import os
import re
import sys
import io
import time
from datetime import date, timedelta
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
NODATA_STREAK_NIGHTS  = 3     # data pull failing this many nights -> flag
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


# ---- 2-3. pull + load  (day-aware: Sunday = FULL reset, weekdays = INCREMENTAL) --------------
FULL_PULL_WEEKDAY = 6     # Mon=0 .. Sun=6.  Sunday -> re-pull the last SELF_HEAL_YEARS and MERGE-upsert
                          # (catches recent splits/divs) — NOT a full 1962 re-pull. Every other day ->
                          # incremental last-7d upsert. Deep history is a ONE-TIME backfill (below).
SELF_HEAL_YEARS = 2       # Sunday split/div self-heal window. Deep history (pre-window) is loaded once
                          # (first run / manual backfill) and never re-fetched — matches the W6 cost-guard
                          # pattern (bounded nightly windows, deep history via one-time backfill).

_PRICE_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
    bigquery.SchemaField("adj_close", "FLOAT64"),
    bigquery.SchemaField("volume", "INT64"),
]


def _load_full(df):
    """WRITE_TRUNCATE the whole price_history (month-partitioned, ticker-clustered)."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    cfg = bigquery.LoadJobConfig(
        schema=_PRICE_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="date"),
        clustering_fields=["ticker"],
    )
    bq.load_table_from_dataframe(df, tbl("price_history"), job_config=cfg).result()


def _merge_recent(df):
    """Upsert the recent window via a staging table + MERGE (no delete-gap; catches EODHD
    same-window revisions). Inlines the min date so the MERGE prunes to recent partitions."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    min_date = df["date"].min().isoformat()
    stg = tbl("price_history_staging")
    bq.load_table_from_dataframe(
        df, stg,
        job_config=bigquery.LoadJobConfig(schema=_PRICE_SCHEMA, write_disposition="WRITE_TRUNCATE"),
    ).result()
    bq.query(f"""
        MERGE `{tbl('price_history')}` T
        USING `{stg}` S
        ON T.ticker = S.ticker AND T.date = S.date AND T.date >= DATE '{min_date}'
        WHEN MATCHED THEN UPDATE SET
          open = S.open, high = S.high, low = S.low, close = S.close,
          adj_close = S.adj_close, volume = S.volume
        WHEN NOT MATCHED THEN INSERT
          (ticker, date, open, high, low, close, adj_close, volume)
          VALUES (S.ticker, S.date, S.open, S.high, S.low, S.close, S.adj_close, S.volume)
    """).result()


def pull_and_load(pull_list):
    first_run = not table_exists("price_history")
    is_sunday = (TODAY.weekday() == FULL_PULL_WEEKDAY)

    # ONE-TIME deep load: only when the table doesn't exist yet (or a manual backfill). Full 1962+ history,
    # WRITE_TRUNCATE. This is the deep backfill — it does NOT recur weekly.
    if first_run:
        print("2. pulling prices — ONE-TIME FULL history (table missing) ...", flush=True)
        df, failed = pullmod.pull_prices(pull_list)
        n = df["ticker"].nunique() if not df.empty else 0
        print(f"   pulled {len(df):,} rows for {n} tickers  | failed: {len(failed)}")
        if df.empty:
            raise RuntimeError("full pull returned no data - aborting (price_history NOT overwritten)")
        print("3. loading price_history (WRITE_TRUNCATE) ...", flush=True)
        _load_full(df)
        return failed

    # SUNDAY self-heal: re-pull only the last SELF_HEAL_YEARS per ticker and MERGE-upsert (catches recent
    # splits/divs). Deep history (pre-window) is left untouched — no weekly 1962 re-pull.
    if is_sunday:
        frm = (TODAY - timedelta(days=365 * SELF_HEAL_YEARS + 5)).isoformat()
        print(f"2. pulling prices — SUNDAY self-heal (last {SELF_HEAL_YEARS}y from {frm}, upsert) ...", flush=True)
        df, failed = pullmod.pull_prices(pull_list, from_date=frm)
        n = df["ticker"].nunique() if not df.empty else 0
        print(f"   pulled {len(df):,} rows for {n} tickers  | failed: {len(failed)}")
        if df.empty:
            print("   !! self-heal pull returned no data — price_history LEFT UNCHANGED.", flush=True)
            return failed
        print("3. merging self-heal window into price_history (upsert) ...", flush=True)
        _merge_recent(df)
        return failed

    # WEEKDAYS: incremental last-7d via bulk (retries inside pull_recent), verify COMPLETE, then upsert.
    print("2. pulling prices — INCREMENTAL (last 7d upsert) ...", flush=True)
    df, complete_ok = pullmod.pull_recent(pull_list, days=7)
    n = df["ticker"].nunique() if not df.empty else 0
    print(f"   recent pull: {len(df):,} rows, {n} tickers  | complete={complete_ok}")
    if df.empty or not complete_ok:
        print("   !! recent pull INCOMPLETE — price_history LEFT UNCHANGED "
              "(next run's 7-day window backfills the gap).", flush=True)
        return []                                  # never overwrite good data with partial
    print("3. merging recent window into price_history (upsert) ...", flush=True)
    _merge_recent(df)
    return []


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
    # no-data streak (still listed, data pull failing)
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

    # candidates: liquid ($50M+ avg $vol) names NOT in the curated universe (top by $vol).
    #   Built weekly by pull_liquid_universe.py -> liquid_universe. Surfaces names to theme/add.
    try:
        CAND_CAP = 300
        curated_norm = {norm(t) for t in curated}
        etf_norm = set()
        if table_exists("etf_universe"):
            etf_norm = {norm(r.ticker) for r in bq.query(
                f"SELECT DISTINCT ticker FROM `{tbl('etf_universe')}` WHERE ticker IS NOT NULL"
            ).result()}
        if table_exists("liquid_universe"):
            cand_total = 0
            for r in bq.query(f"""SELECT ticker, name, sector, avg_dollar_vol
                                  FROM `{tbl('liquid_universe')}` ORDER BY avg_dollar_vol DESC""").result():
                nt = norm(r.ticker)
                if nt in curated_norm or nt in etf_norm:
                    continue
                cand_total += 1
                if cand_total <= CAND_CAP:
                    review_rows.append({
                        "section": "candidate", "ticker": r.ticker, "name": r.name, "themes": None,
                        "detail": f"${(r.avg_dollar_vol or 0)/1e6:,.0f}M avg $vol"
                                  + (f" · {r.sector}" if r.sector else "")
                                  + " — not in universe; theme + add",
                        "as_of": TODAY,
                    })
            print(f"   candidates: {cand_total} liquid names not in universe"
                  + (f" (showing top {CAND_CAP})" if cand_total > CAND_CAP else ""))
    except Exception as e:
        print("   (candidate screen skipped:", e, ")")

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
    # 0. apply queued ticker_review Submits (theme-adds / removes) BEFORE reading the universe,
    #    so an add is in tickers.txt for tonight's pull + theme rebuilds and a remove drops out.
    #    Wrapped so a reconcile failure can NEVER abort the nightly.
    t = clk()
    try:
        import apply_review_actions
        apply_review_actions.reconcile()
    except Exception as e:
        print("0. (apply_review_actions skipped:", e, ")", flush=True)
    T['0 apply review actions'] = clk() - t
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
