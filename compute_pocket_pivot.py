"""
Compute the POCKET PIVOT detector features + history for the full universe -> BigQuery.
Full-history backfill AND (as-is) nightly. Mirrors compute_flat_base_consolidation.py.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,VOLUME)
Writes: stonks_data.pocket_pivot_features  (latest row per ticker -> dashboard / focus list)
        stonks_data.pocket_pivot_history    (point-in-time daily -> v_pocket_pivot_entries / backtest)

Prices SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close on O/H/L). VOLUME kept RAW:
the pocket-pivot volume test is LOCAL (prior 10 days, sign-based) and the Force Index sign
follows Δclose — split-adjusting volume would only matter within ~10 days of a split (rare,
negligible for this signal). History written in batches. Run: python compute_pocket_pivot.py

🚨 DEPENDENCY: price_history must expose a `volume` column (EODHD OHLCV). If it is named
differently, change the SELECT + adjust_ohlcv below.
"""
import pandas as pd
from google.cloud import bigquery
from pocket_pivot import run_pocket_pivot

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True
MIN_BARS = 60            # need trend_ma(50) + FI(13) + lookback(10) to be well-formed
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.pocket_pivot_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.pocket_pivot_features"

# exact per-bar output of run_pocket_pivot() + ticker
FEATURE_COLS = [
    "ticker", "date", "close", "volume", "sma10", "sma50", "up_day",
    "down_vol_max", "vol_ratio", "dist_ma_pct", "upper_half",
    "force_index", "fi_positive", "atr_pct", "vcp_tight", "recent_vdu", "pocket_pivot", "pp_type",
]


def adjust_ohlcv(g):
    """Adjust O/H/L/close by adj_close/close; keep volume RAW (see module docstring)."""
    if not ADJUST:
        return g[["date", "open", "high", "low", "close", "volume"]].copy()
    f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
    return pd.DataFrame({
        "date": g["date"], "open": g["open"] * f, "high": g["high"] * f,
        "low": g["low"] * f, "close": g["adj_close"], "volume": g["volume"],
    })


def compute_ticker(tkr, g):
    g = g.sort_values("date").reset_index(drop=True)
    H = run_pocket_pivot(adjust_ohlcv(g))            # validated defaults (offset 5%, fi 13)
    H["ticker"] = tkr
    return H


def main():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT ticker, date, open, high, low, close, adj_close, volume
        FROM `{PROJECT}.{DATASET}.price_history`
        WHERE date < CURRENT_DATE() AND date >= '1990-01-01'
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
    print(f"Done: {processed:,} tickers -> pocket_pivot_history + pocket_pivot_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
