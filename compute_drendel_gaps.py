"""
Compute Drendel gap features + history for the full universe, DAILY and WEEKLY,
and write to BigQuery. Use as the one-time full-history backfill AND (as-is) nightly.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,volume)
Writes: stonks_data.gap_features   (latest row per ticker per timeframe -> dashboard)
        stonks_data.gap_history    (point-in-time, one row per ticker/date/tf -> backtest)

Prices are SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close applied to O/H/L) so
historical splits do not create false gaps. Set ADJUST=False for raw OHLC.

History is written in batches of BATCH_TICKERS to keep memory low on CI runners.

Run: python compute_drendel_gaps.py
Requires: pandas, google-cloud-bigquery  (google-cloud-bigquery-storage optional, faster read)
"""
import pandas as pd
from google.cloud import bigquery
from drendel_gaps import run_drendel, resample_weekly

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True          # split+div adjust OHLC (recommended for history). False = raw.
MIN_BARS = 2            # skip tickers with fewer than this many bars
BATCH_TICKERS = 500     # flush gap_history to BQ every N tickers (memory cap)

HIST_TABLE = f"{PROJECT}.{DATASET}.gap_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.gap_features"

FEATURE_COLS = [
    "ticker", "tf", "date", "close",
    "gap_up_today", "gap_down_today", "new_gap_size_pct",
    "support_gaps", "resistance_gaps", "net_gap_bias",
    "support_gaps_near", "resistance_gaps_near", "net_gap_bias_near",
    "in_support_gap", "in_resistance_gap",
    "nearest_support_top", "nearest_support_bottom", "pct_to_support",
    "nearest_resistance_bottom", "nearest_resistance_top", "pct_to_resistance",
    "days_since_gap_up", "days_since_gap_down",
    # --- gap-type flags on the nearest zone ---
    "nearest_support_base", "nearest_support_shakeout", "nearest_support_wedge_pop", "nearest_support_earnings",
    "nearest_resistance_base", "nearest_resistance_shakeout", "nearest_resistance_wedge_pop", "nearest_resistance_earnings",
    # --- per-type counts of nearby (+/-25%) zones ---
    "near_support_base_n", "near_support_shakeout_n", "near_support_wedge_pop_n", "near_support_earnings_n",
    "near_resistance_base_n", "near_resistance_shakeout_n", "near_resistance_wedge_pop_n", "near_resistance_earnings_n",
]


def adjust_ohlc(g):
    """Return date+OHLC (+volume), split+div adjusted if ADJUST else raw."""
    if not ADJUST:
        out = g[["date", "open", "high", "low", "close"]].copy()
    else:
        f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
        out = pd.DataFrame({
            "date":  g["date"],
            "open":  g["open"]  * f,
            "high":  g["high"]  * f,
            "low":   g["low"]   * f,
            "close": g["adj_close"],     # == close * f
        })
    if "volume" in g.columns:
        out["volume"] = g["volume"].values   # raw volume (for the earnings vol ratio)
    return out


def compute_ticker(tkr, g):
    """Return (history_df, feature_df) for one ticker, daily + weekly."""
    g = g.sort_values("date").reset_index(drop=True)
    px = adjust_ohlc(g)
    hist_frames, feat_frames = [], []

    _, hD, _, _ = run_drendel(px)          # daily
    hD["ticker"] = tkr
    hD["tf"] = "D"
    hist_frames.append(hD)
    feat_frames.append(hD.iloc[[-1]].copy())

    w = resample_weekly(px)                # weekly
    if len(w) >= MIN_BARS:
        _, hW, _, _ = run_drendel(w)
        hW["ticker"] = tkr
        hW["tf"] = "W"
        hist_frames.append(hW)
        feat_frames.append(hW.iloc[[-1]].copy())

    return (pd.concat(hist_frames, ignore_index=True),
            pd.concat(feat_frames, ignore_index=True))


def main():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT ticker, date, open, high, low, close, adj_close, volume
        FROM `{PROJECT}.{DATASET}.price_history`
        WHERE date < CURRENT_DATE()
          AND date >= '1998-01-01'
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
                clustering_fields=["ticker", "tf"],
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

    flush_history()   # final partial batch

    feat = pd.concat(feat_all, ignore_index=True)[FEATURE_COLS]
    feat["date"] = pd.to_datetime(feat["date"])
    client.load_table_from_dataframe(
        feat, FEAT_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            clustering_fields=["ticker", "tf"],
        ),
    ).result()

    print(f"Done: {processed:,} tickers -> gap_history + gap_features ({len(feat):,} feature rows).")


if __name__ == "__main__":
    main()
