"""
compute_parabolic_short.py — Parabolic Short features + history for the full universe (DAILY),
write to BigQuery. One-time full-history backfill AND (as-is) nightly.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,volume)
Writes: stonks_data.parabolic_features (latest row per ticker -> dashboard)
        stonks_data.parabolic_history  (point-in-time, one row per ticker/date -> backtest)

Prices SPLIT+DIV ADJUSTED (factor = adj_close/close) so historical splits/divs don't distort
extension/ATR (the raw-CSV AIG-1981 ext=21 artifact). DAILY only (parabolic = day-of setup).

Run: python compute_parabolic_short.py
Requires: pandas, google-cloud-bigquery  (google-cloud-bigquery-storage optional)
"""
import pandas as pd
from google.cloud import bigquery
from parabolic_short import run_parabolic_short

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST = True
MIN_BARS = 60           # need 50-SMA + peak windows
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.parabolic_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.parabolic_features"

FEATURE_COLS = [
    "ticker", "date", "close",
    "ext_atr_50", "pct_above_50", "run_pct", "up_streak", "armed", "parabolic_short",
    "rsi", "stretch5", "bb_out", "gap_run",
    "ext_peak", "rsi_peak", "stretch_peak", "gap_peak", "run_peak",
    "bearish_candle", "avwap",
    # day-of (intraday-prep)
    "prior_day_low", "prior_day_high", "gap_up_fade", "first_break_prior_low",
    # pre-anticipation (imminence)
    "pct_to_prior_low", "pct_to_prior_high", "pct_from_peak", "pct_vs_avwap", "below_avwap",
    "stop", "grade",
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


def compute_ticker(tkr, g):
    g = g.sort_values("date").reset_index(drop=True)
    out = run_parabolic_short(adjust_ohlc(g))
    out["ticker"] = tkr
    hist = out[FEATURE_COLS].copy()
    return hist, hist.iloc[[-1]].copy()


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
            h, f = compute_ticker(tkr, g)
            hist_buf.append(h)
            feat_all.append(f)
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

    print(f"Done: {processed:,} tickers -> parabolic_history + parabolic_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
