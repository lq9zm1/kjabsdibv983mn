"""
Compute Drendel gap features + history for the full universe, DAILY and WEEKLY,
and write to BigQuery. Use as the one-time full-history backfill AND (as-is) nightly.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,volume)
Writes: stonks_data.gap_features   (latest row per ticker per timeframe -> dashboard)
        stonks_data.gap_history    (point-in-time, one row per ticker/date/tf -> backtest)

Prices are SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close applied to O/H/L) so
historical splits do not create false gaps. Set ADJUST=False for raw OHLC.

Run: python compute_drendel_gaps.py
Requires: pandas, google-cloud-bigquery, google-cloud-bigquery-storage
"""
import pandas as pd
from google.cloud import bigquery
from drendel_gaps import run_drendel, resample_weekly

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True          # split+div adjust OHLC (recommended for history). False = raw.
MIN_BARS = 2            # skip tickers with fewer than this many bars

FEATURE_COLS = [
    "ticker", "tf", "date", "close",
    "gap_up_today", "gap_down_today", "new_gap_size_pct",
    "support_gaps", "resistance_gaps", "net_gap_bias",
    "support_gaps_near", "resistance_gaps_near", "net_gap_bias_near",
    "in_support_gap", "in_resistance_gap",
    "nearest_support_top", "nearest_support_bottom", "pct_to_support",
    "nearest_resistance_bottom", "nearest_resistance_top", "pct_to_resistance",
    "days_since_gap_up", "days_since_gap_down",
]


def adjust_ohlc(g):
    """Return date+OHLC, split+div adjusted if ADJUST else raw."""
    if not ADJUST:
        return g[["date", "open", "high", "low", "close"]].copy()
    f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
    return pd.DataFrame({
        "date":  g["date"],
        "open":  g["open"]  * f,
        "high":  g["high"]  * f,
        "low":   g["low"]   * f,
        "close": g["adj_close"],     # == close * f
    })


def compute_ticker(tkr, g):
    """Return (history_df, feature_df) for one ticker, daily + weekly."""
    g = g.sort_values("date").reset_index(drop=True)
    px = adjust_ohlc(g)
    hist_frames, feat_frames = [], []

    # daily
    _, hD, _, _ = run_drendel(px)
    hD["ticker"] = tkr
    hD["tf"] = "D"
    hist_frames.append(hD)
    feat_frames.append(hD.iloc[[-1]].copy())

    # weekly
    w = resample_weekly(px)
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
        ORDER BY ticker, date
    """
    print("Reading price_history ...")
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)
    print(f"  {len(df):,} rows, {df['ticker'].nunique():,} tickers")

    hist_all, feat_all = [], []
    for i, (tkr, g) in enumerate(df.groupby("ticker", sort=False)):
        if len(g) < MIN_BARS:
            continue
        try:
            h, f = compute_ticker(tkr, g)
            hist_all.append(h)
            feat_all.append(f)
        except Exception as e:
            print(f"  !! {tkr}: {e}")
        if (i + 1) % 250 == 0:
            print(f"  {i + 1} tickers processed")

    hist = pd.concat(hist_all, ignore_index=True)[FEATURE_COLS]
    feat = pd.concat(feat_all, ignore_index=True)[FEATURE_COLS]
    hist["date"] = pd.to_datetime(hist["date"])
    feat["date"] = pd.to_datetime(feat["date"])
    print(f"gap_history rows={len(hist):,}   gap_features rows={len(feat):,}")

    # write gap_history (partitioned by month, clustered)
    client.load_table_from_dataframe(
        hist, f"{PROJECT}.{DATASET}.gap_history",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(field="date", type_="MONTH"),
            clustering_fields=["ticker", "tf"],
        ),
    ).result()

    # write gap_features (small, clustered)
    client.load_table_from_dataframe(
        feat, f"{PROJECT}.{DATASET}.gap_features",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            clustering_fields=["ticker", "tf"],
        ),
    ).result()

    print("Done: wrote gap_history + gap_features.")


if __name__ == "__main__":
    main()
