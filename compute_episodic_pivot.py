"""
Compute the EPISODIC PIVOT (EP) detector -> BigQuery. Full-history backfill AND nightly.
Mirrors compute_pocket_pivot.py.

Reads : stonks_data.price_history  (ticker,date,open,high,low,close,adj_close,VOLUME)
Writes: stonks_data.episodic_pivot_history   (every ARMED EP event, point-in-time)
        stonks_data.episodic_pivot_features   (latest ARMED EP per ticker -> dashboard)

⚠️ LAYER A ONLY (the OHLCV scan). The catalyst/fundamental layer B (earnings surprise+
growth, float, coverage, ownership, newsletters) is DEFERRED to the refinement layer
(EODHD Fundamentals is a paid upcharge) — see REV-C16/C17. This table is the ARMED
candidate list; layer B will rank it later.

Prices SPLIT+DIVIDEND ADJUSTED (factor = adj_close/close on O/H/L) for correct %-moves
and volume ratios across splits. VOLUME kept RAW. The min_price liquidity floor is applied
to RAW close (`raw_close`), NOT adjusted — else massively-split winners (e.g. NVDA at ~$1
adjusted) get wrongly cut.

Unlike PP, only ARMED rows are written (EP fires ~0.1-1% of bars — storing every bar is
wasteful). Run: python compute_episodic_pivot.py
"""
import pandas as pd
from google.cloud import bigquery
from episodic_pivot import run_episodic_pivot

PROJECT = "stonks-498420"
DATASET = "stonks_data"
ADJUST  = True
MIN_BARS = 60            # avgv50 well-formed; young names flagged is_ipo_window
BATCH_TICKERS = 500

HIST_TABLE = f"{PROJECT}.{DATASET}.episodic_pivot_history"
FEAT_TABLE = f"{PROJECT}.{DATASET}.episodic_pivot_features"

# per-armed-event output of run_episodic_pivot() + ticker (every row here IS an ARMED EP)
FEATURE_COLS = [
    "ticker", "date", "close", "volume",
    "pct_up", "gap_pct", "vol_ratio",
    "vol_252high", "range_exp", "prior_6mo_ret", "neglect_proxy", "pos_52w", "is_ipo_window",
    "force_index", "fi_positive",
    "trigger_level", "stop",
]


def adjust_ohlcv(g):
    """Adjust O/H/L/close by adj_close/close; keep volume RAW; keep raw_close for the floor."""
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
    H = run_episodic_pivot(adjust_ohlcv(g), price_floor_col="raw_close")  # validated defaults
    H = H[H["ep_armed"]].copy()                     # keep only ARMED EP events
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
                feat_all.append(H.iloc[[-1]].copy())   # latest ARMED EP for this ticker
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
    print(f"Done: {processed:,} tickers scanned -> episodic_pivot_history + "
          f"episodic_pivot_features ({len(feat):,} tickers with an EP).")


if __name__ == "__main__":
    main()
