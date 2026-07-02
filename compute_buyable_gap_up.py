"""
Compute the BUYABLE GAP-UP (BGU) detector -> BigQuery. Full-history backfill AND nightly.
Mirrors compute_episodic_pivot.py.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,VOLUME)
Writes: stonks_data.buyable_gap_up_history   (every ARMED BGU event = Morales gap+vol, point-in-time)
        stonks_data.buyable_gap_up_features   (latest ARMED BGU per ticker -> dashboard)

⚠️ LAYER A ONLY (OHLCV). Detection = Morales's 2 hard rules (gap>=0.75*ATR40[prior] + vol>=1.5*avg50);
the quality filters (close>=open, unfilled rising window) drive the GRADE in v_buyable_gap_up_entries,
not detection (REV-C21/C22). The "fundamentally sound / catalyst" part -> refinement layer.

Prices SPLIT+DIV ADJUSTED (factor=adj_close/close on O/H/L); VOLUME raw; min_price floor on RAW close.
Only ARMED rows written (BGU ~1% of bars). Run: python compute_buyable_gap_up.py
"""
import pandas as pd
from google.cloud import bigquery
from buyable_gap_up import run_buyable_gap_up

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True
MIN_BARS = 60
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.buyable_gap_up_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.buyable_gap_up_features"

FEATURE_COLS = [
    "ticker", "date", "close", "volume",
    "gap_pts", "gap_atr", "vol_ratio",
    "unfilled", "bullish_hold", "constructive", "above_200", "bgu_type",
    "force_index", "fi_positive",
    "entry_ref", "stop",
]


def adjust_ohlcv(g):
    if not ADJUST:
        out = g[["date", "open", "high", "low", "close", "volume"]].copy()
        out["raw_close"] = g["close"]
        return out
    f = (g["adj_close"] / g["close"]).where(g["close"] > 0, 1.0)
    return pd.DataFrame({
        "date": g["date"], "open": g["open"] * f, "high": g["high"] * f,
        "low": g["low"] * f, "close": g["adj_close"], "volume": g["volume"],
        "raw_close": g["close"],
    })


def compute_ticker(tkr, g):
    g = g.sort_values("date").reset_index(drop=True)
    H = run_buyable_gap_up(adjust_ohlcv(g), price_floor_col="raw_close")   # Morales scan (gap+vol)
    H = H[H["bgu_armed"]].copy()
    H["ticker"] = tkr
    return H


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
        chunk = pd.concat(hist_buf, ignore_index=True)
        if chunk.empty:
            hist_buf = []
            return
        chunk = chunk[FEATURE_COLS]
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
            if not H.empty:
                hist_buf.append(H)
                feat_all.append(H.iloc[[-1]].copy())
        except Exception as e:
            print(f"  !! {tkr}: {e}")
        processed += 1
        if processed % BATCH_TICKERS == 0:
            flush_history()
            print(f"  {processed} tickers scanned")

    flush_history()

    feat = pd.concat(feat_all, ignore_index=True)[FEATURE_COLS]
    feat["date"] = pd.to_datetime(feat["date"])
    client.load_table_from_dataframe(
        feat, FEAT_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", clustering_fields=["ticker"]),
    ).result()
    print(f"Done: {processed:,} tickers -> buyable_gap_up_history + buyable_gap_up_features "
          f"({len(feat):,} tickers with a BGU).")


if __name__ == "__main__":
    main()
