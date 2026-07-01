"""
Compute the UNIFIED CONSOLIDATION / FLAT-BASE detector features + history for the
full universe -> BigQuery. Full-history backfill AND (as-is) nightly.
Mirrors compute_flat_base.py (which this supersedes).

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close)
Writes: stonks_data.flat_base_consolidation_features  (latest row per ticker -> dashboard / focus list)
        stonks_data.flat_base_consolidation_history    (point-in-time daily -> v_flat_base_consolidation_entries / backtest)

Prices SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close on O/H/L). History written in
batches of BATCH_TICKERS. Run: python compute_flat_base_consolidation.py
"""
import pandas as pd
from google.cloud import bigquery
from flat_base_consolidation import run_flat_base_consolidation

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True
MIN_BARS = 150            # need prior_lb(103) + pole/base room
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.flat_base_consolidation_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.flat_base_consolidation_features"

# exact per-bar output of run_flat_base_consolidation() + ticker
FEATURE_COLS = [
    "ticker", "date", "in_consol", "state", "top", "low", "depth_pct",
    "len_days", "len_class", "shape", "pole_pct", "pivot_entry",
    "cheat_entry", "breakout",
]


def adjust_ohlc(g):
    if not ADJUST:
        return g[["date", "open", "high", "low", "close"]].copy()
    f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
    return pd.DataFrame({
        "date": g["date"], "open": g["open"] * f, "high": g["high"] * f,
        "low": g["low"] * f, "close": g["adj_close"],
    })


def compute_ticker(tkr, g):
    g = g.sort_values("date").reset_index(drop=True)
    H = run_flat_base_consolidation(adjust_ohlc(g))   # validated defaults
    H["ticker"] = tkr
    return H


def main():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT ticker, date, open, high, low, close, adj_close
        FROM `{PROJECT}.{DATASET}.price_history`
        WHERE date < CURRENT_DATE() AND date >= '1998-01-01'
        ORDER BY ticker, date
    """
    print("Reading price_history ...")
    try:
        df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        print(f"  Storage API unavailable ({type(e).__name__}); standard read.")
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
            write_disposition="WRITE_TRUNCATE", clustering_fields=["ticker"]),
    ).result()
    print(f"Done: {processed:,} tickers -> flat_base_consolidation_history + flat_base_consolidation_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
