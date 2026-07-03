"""
compute_hvc_base_break.py — HVC-linked early base-break (#12) features + history (DAILY) -> BigQuery.
Reads price_history (adj OHLC, >=1998), writes hvc_base_break_features (latest/ticker) +
hvc_base_break_history (point-in-time). No benchmark (grade = early Stage-2, applied in the view).

Run: python compute_hvc_base_break.py
Requires: pandas, google-cloud-bigquery, db-dtypes
"""
import pandas as pd
from google.cloud import bigquery
from hvc_base_break import run_hvc_base_break

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST = True
MIN_BARS = 200
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.hvc_base_break_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.hvc_base_break_features"

FEATURE_COLS = [
    "ticker", "date", "close", "hvc_level", "is_1b_break", "rising_lows",
    "days_since_hvc", "pullback_depth", "tight_ma_stop", "stop", "risk_pct", "ext_atr_50",
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
            out = run_hvc_base_break(adjust_ohlc(g.sort_values("date")))
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
    print(f"Done: {processed:,} tickers -> hvc_base_break_history + hvc_base_break_features ({len(feat):,} rows).")


if __name__ == "__main__":
    main()
