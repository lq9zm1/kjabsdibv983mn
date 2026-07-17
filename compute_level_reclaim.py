"""
compute_level_reclaim.py — Level Reclaim / UNR features + history (DAILY) -> BigQuery.
Reads price_history (adj OHLC, >=1998), loads SPY for RS, writes level_reclaim_features (latest/ticker)
+ level_reclaim_history (point-in-time). Grade (gap-support-at-MA + RS) applied in the view.

Run: python compute_level_reclaim.py
Requires: pandas, google-cloud-bigquery, db-dtypes
"""
import pandas as pd
from google.cloud import bigquery
from level_reclaim import run_level_reclaim

PROJECT = "stonks-498420"
DATASET = "stonks_data"
BENCHMARK = "SPY"
ADJUST = True
MIN_BARS = 220
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.level_reclaim_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.level_reclaim_features"

FEATURE_COLS = [
    "ticker", "date", "close", "is_reclaim", "which_ma", "is_200ma_reclaim",
    "undercut_low", "undercut_bars", "stop", "risk_pct", "rs_rising", "ext_atr_50",
]


def adjust_ohlc(g):
    if not ADJUST:
        out = g[["date", "open", "high", "low", "close"]].copy()
    else:
        f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
        out = pd.DataFrame({"date": g["date"], "open": g["open"] * f, "high": g["high"] * f,
                            "low": g["low"] * f, "close": g["adj_close"]})
    out["volume"] = g["volume"].values
    return out


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

    bench = df[df["ticker"] == BENCHMARK][["date", "adj_close"]].rename(columns={"adj_close": "spy_close"})
    if bench.empty:
        print(f"  !! benchmark {BENCHMARK} missing -> rs_rising NULL")

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
            out = run_level_reclaim(px)
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
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", clustering_fields=["ticker"]),
    ).result()
    print(f"Done: {processed:,} tickers -> level_reclaim_history + level_reclaim_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
