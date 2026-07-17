"""
compute_hvc.py — HVC features + history for the full universe (DAILY), write to BigQuery.
One-time full-history backfill AND (as-is) nightly.

Reads : stonks_data.price_history (ticker,date,open,high,low,close,adj_close,volume)
Writes: stonks_data.hvc_features (latest row per ticker -> dashboard)
        stonks_data.hvc_history  (point-in-time, one row per ticker/date -> backtest)

Prices SPLIT+DIV ADJUSTED (factor = adj_close/close). RS = ticker adj_close / SPY adj_close
(BENCHMARK below), aligned by date, feeds the grade's rs_rising. DAILY only (HVC = day event).

Run: python compute_hvc.py
Requires: pandas, google-cloud-bigquery, db-dtypes  (bigquery-storage optional)
"""
import pandas as pd
from google.cloud import bigquery
from hvc import run_hvc

PROJECT = "stonks-498420"
DATASET = "stonks_data"
BENCHMARK = "SPY"       # RS denominator; swap to your index ticker if SPY isn't in price_history
ADJUST = True
MIN_BARS = 200          # need SMA200 + RS
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.hvc_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.hvc_features"

FEATURE_COLS = [
    "ticker", "date", "close", "volume", "vol_ratio", "close_pos", "gap",
    "is_hvc", "is_gap_hvc", "hvc_level", "above_hvc", "pct_to_hvc",
    "above_10", "above_20", "above_50", "above_200", "ext_atr_50",
    "days_since_hvc", "ft_hold", "rs_rising", "ft_confirmed", "grade",
]


def adjust_ohlc(g):
    if not ADJUST:
        out = g[["date", "open", "high", "low", "close"]].copy()
    else:
        f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
        out = pd.DataFrame({
            "date":  g["date"],
            "open":  g["open"] * f,
            "high":  g["high"] * f,
            "low":   g["low"] * f,
            "close": g["adj_close"],
        })
    out["volume"] = g["volume"].values
    return out


def main():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT ticker, date, open, high, low, close, adj_close, volume
        FROM `{PROJECT}.{DATASET}.price_history`
        WHERE date < CURRENT_DATE()
          AND date >= '1990-01-01'
        ORDER BY ticker, date
    """
    print("Reading price_history ...")
    try:
        df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        print(f"  Storage API unavailable ({type(e).__name__}); standard read.")
        df = client.query(sql).result().to_dataframe(create_bqstorage_client=False)
    print(f"  {len(df):,} rows, {df['ticker'].nunique():,} tickers")

    # benchmark adj_close by date (for RS)
    bench = df[df["ticker"] == BENCHMARK][["date", "adj_close"]].rename(columns={"adj_close": "spy_close"})
    if bench.empty:
        print(f"  !! benchmark {BENCHMARK} not in price_history -> rs_rising will be NULL (grade = MA-only)")

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
            px = adjust_ohlc(g.sort_values("date"))
            if not bench.empty:
                px = px.merge(bench, on="date", how="left")
            out = run_hvc(px)
            out["ticker"] = tkr
            hist = out[FEATURE_COLS].copy()
            hist_buf.append(hist)
            feat_all.append(hist.iloc[[-1]].copy())
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

    print(f"Done: {processed:,} tickers -> hvc_history + hvc_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
