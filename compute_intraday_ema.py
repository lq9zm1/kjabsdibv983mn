"""
compute_intraday_ema.py — resample price_intraday 1m -> 5m, compute EXACT EMA6/EMA20 on 5m closes,
write to BigQuery intraday_ema.

Reads : stonks_data.price_intraday  (1m bars; regular session filtered here)
Writes: stonks_data.intraday_ema    (one row per ticker/5m bar; point-in-time = backtest-safe)

EMA is exact (pandas .ewm(span=N, adjust=False)) per ticker per DAY (resets each session,
matching intraday EMA convention). Regular session only: 13:30-20:00 UTC (9:30-16:00 ET).

Run: python compute_intraday_ema.py
Requires: pandas, google-cloud-bigquery, db-dtypes
"""
import pandas as pd
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
SRC   = f"{PROJECT}.{DATASET}.price_intraday"
TABLE = f"{PROJECT}.{DATASET}.intraday_ema"

EMA_FAST = 6
EMA_SLOW = 20


def main():
    client = bigquery.Client(project=PROJECT)

    # 1m regular-session bars -> pandas
    q = f"""
      SELECT ticker, dt_utc, open, high, low, close, volume
      FROM `{SRC}`
      WHERE `interval` = '1m'
        AND TIME(dt_utc) BETWEEN '13:30:00' AND '20:00:00'
    """
    df = client.query(q).to_dataframe()
    if df.empty:
        print("No 1m bars found.")
        return
    df["dt_utc"] = pd.to_datetime(df["dt_utc"], utc=True)
    df["d"] = df["dt_utc"].dt.date

    # resample 1m -> 5m per (ticker, day); label bar by its 5m bucket start
    out = []
    for (tk, d), g in df.groupby(["ticker", "d"]):
        g = g.sort_values("dt_utc").set_index("dt_utc")
        bars5 = pd.DataFrame({
            "open":   g["open"].resample("5min").first(),
            "high":   g["high"].resample("5min").max(),
            "low":    g["low"].resample("5min").min(),
            "close":  g["close"].resample("5min").last(),
            "volume": g["volume"].resample("5min").sum(),
        }).dropna(subset=["close"])
        if bars5.empty:
            continue
        # EXACT EMA on 5m closes (resets per session)
        bars5["ema6"]  = bars5["close"].ewm(span=EMA_FAST, adjust=False).mean()
        bars5["ema20"] = bars5["close"].ewm(span=EMA_SLOW, adjust=False).mean()
        bars5["ticker"] = tk
        bars5["trade_date"] = d
        bars5 = bars5.reset_index().rename(columns={"dt_utc": "dt_utc"})
        out.append(bars5)

    res = pd.concat(out, ignore_index=True)
    res["dt_utc"] = pd.to_datetime(res["dt_utc"], utc=True)
    res["above_ema6"]  = res["close"] > res["ema6"]
    res["above_ema20"] = res["close"] > res["ema20"]
    res["pct_to_ema6"]  = (res["close"] / res["ema6"]  - 1) * 100
    res["pct_to_ema20"] = (res["close"] / res["ema20"] - 1) * 100
    keep = ["ticker", "trade_date", "dt_utc", "open", "high", "low", "close", "volume",
            "ema6", "ema20", "above_ema6", "above_ema20", "pct_to_ema6", "pct_to_ema20"]
    res = res[keep]

    client.load_table_from_dataframe(
        res, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # clean reload each run
            time_partitioning=bigquery.TimePartitioning(field="dt_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(res)} 5m EMA bars -> intraday_ema (EMA{EMA_FAST}/EMA{EMA_SLOW}, exact).")


if __name__ == "__main__":
    main()
