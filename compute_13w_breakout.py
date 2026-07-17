"""
Compute 13-Week Breakout -> 21-EMA Pullback features + history for the full universe
and write to BigQuery. Full-history backfill AND (as-is) nightly.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,volume)
Writes: stonks_data.breakout_features   (latest row per ticker -> dashboard)
        stonks_data.breakout_history     (point-in-time daily -> backtest)

Prices are SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close applied to O/H/L) so
splits don't create false breakouts / EMA jumps. Set ADJUST=False for raw OHLC.
History is written in batches of BATCH_TICKERS to keep memory low on CI runners.

Run: python compute_13w_breakout.py
Requires: pandas, google-cloud-bigquery
"""
import pandas as pd
from google.cloud import bigquery
from breakout_13w import run_breakout

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True
MIN_BARS = 70            # need ~65 for the high + EMA warmup
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.breakout_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.breakout_features"

FEATURE_COLS = [
    "ticker", "date", "close", "ema_21", "pct_to_21ema",
    "bo_daily", "bo_weekly",
    "days_since_bo_daily", "days_since_bo_weekly",
    "in_watch_daily", "in_watch_weekly", "in_watch",
    "pullback_entry",
]


def adjust_ohlc(g):
    if not ADJUST:
        return g[["date", "open", "high", "low", "close"]].copy()
    f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
    return pd.DataFrame({
        "date":  g["date"],
        "open":  g["open"]  * f,
        "high":  g["high"]  * f,
        "low":   g["low"]   * f,
        "close": g["adj_close"],
    })


def compute_ticker(tkr, g):
    g = g.sort_values("date").reset_index(drop=True)
    px = adjust_ohlc(g)
    H, _, _ = run_breakout(px)
    H["ticker"] = tkr
    return H


def main():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT ticker, date, open, high, low, close, adj_close
        FROM `{PROJECT}.{DATASET}.price_history`
        WHERE date < CURRENT_DATE()
          AND date >= '1990-01-01'
        ORDER BY ticker, date
    """
    print("Reading price_history ...")
    try:
        df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        print(f"  Storage API unavailable ({type(e).__name__}); using standard read.")
        df = client.query(sql).result().to_dataframe(create_bqstorage_client=False)
    print(f"  {len(df):,} rows, {df['ticker'].nunique():,} tickers")

    hist_buf, feat_all = [], []
    first_write = True
    processed = 0

    def flush_history():
        nonlocal hist_buf, first_write
        if not hist_buf:
            return
        chunk = pd.concat(hist_buf, ignore_index=True)[FEATURE_COLS]
        chunk["date"] = pd.to_datetime(chunk["date"])
        client.load_table_from_dataframe(
            chunk, HIST_TABLE,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE" if first_write else "WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(field="date", type_="MONTH"),
                clustering_fields=["ticker"],
            ),
        ).result()
        first_write = False
        hist_buf = []

    for tkr, g in df.groupby("ticker", sort=False):
        if len(g) < MIN_BARS:
            continue
        try:
            H = compute_ticker(tkr, g)
            hist_buf.append(H)
            feat_all.append(H.iloc[[-1]].copy())
        except Exception as e:
            print(f"  !! {tkr}: {e}")
        processed += 1
        if processed % BATCH_TICKERS == 0:
            flush_history()
            print(f"  {processed} tickers written")

    flush_history()

    feat = pd.concat(feat_all, ignore_index=True)[FEATURE_COLS]
    feat["date"] = pd.to_datetime(feat["date"])
    client.load_table_from_dataframe(
        feat, FEAT_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            clustering_fields=["ticker"],
        ),
    ).result()

    print(f"Done: {processed:,} tickers -> breakout_history + breakout_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
