"""
compute_realtime_bars.py — build LIVE intraday metrics from the realtime_quotes snapshot series.
Reads : stonks_data.realtime_quotes  (15-min /real-time snapshots, APPEND history)
Writes: stonks_data.realtime_bars     (one row per ticker/snapshot; point-in-time = backtest-safe)

Per ticker per DAY (regular session 13:30-20:00 UTC), ordered by quote_utc:
  vol_delta   = cum_volume - prev cum_volume   (per-snapshot volume)
  ema6/ema20  = EXACT pandas ewm(span, adjust=False) on snapshot close series
  vwap_rt     = running Sum(close*vol_delta)/Sum(vol_delta)  (APPROX — snapshot granularity)
  rvol_tod    = today cum_volume / avg cum_volume at same snapshot-of-day over prior days

Run: python compute_realtime_bars.py
Requires: pandas, google-cloud-bigquery, db-dtypes
"""
import pandas as pd
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
SRC   = f"{PROJECT}.{DATASET}.realtime_quotes"
TABLE = f"{PROJECT}.{DATASET}.realtime_bars"

EMA_FAST, EMA_SLOW = 6, 20


def main():
    client = bigquery.Client(project=PROJECT)
    q = f"""
      SELECT ticker, quote_utc, close, high, low, volume
      FROM `{SRC}`
      WHERE TIME(quote_utc) BETWEEN '13:30:00' AND '20:00:00'
    """
    df = client.query(q).to_dataframe()
    if df.empty:
        print("No realtime snapshots yet."); return
    df["quote_utc"] = pd.to_datetime(df["quote_utc"], utc=True)
    df["d"] = df["quote_utc"].dt.date
    # dedupe (APPEND boundary): one row per ticker+quote_utc
    df = df.sort_values("quote_utc").drop_duplicates(subset=["ticker", "quote_utc"])

    out = []
    for (tk, d), g in df.groupby(["ticker", "d"]):
        g = g.sort_values("quote_utc").copy()
        g["vol_delta"] = g["volume"].diff().clip(lower=0).fillna(g["volume"])
        # exact EMA on snapshot closes (resets per session)
        g["ema6"]  = g["close"].ewm(span=EMA_FAST, adjust=False).mean()
        g["ema20"] = g["close"].ewm(span=EMA_SLOW, adjust=False).mean()
        # approx running VWAP from snapshot close * vol_delta
        cum_pv = (g["close"] * g["vol_delta"]).cumsum()
        cum_v  = g["vol_delta"].cumsum()
        g["vwap_rt"] = cum_pv / cum_v.replace(0, pd.NA)
        out.append(g)
    res = pd.concat(out, ignore_index=True)

    # time-of-day RVOL: today cum_volume vs avg cum_volume at same quote-time over prior days
    res["tod"] = res["quote_utc"].dt.strftime("%H:%M")
    base = (res.groupby(["ticker", "tod", "d"])["volume"].last().reset_index())
    base = base.rename(columns={"volume": "cum_vol"})
    base["avg_prior"] = (base.sort_values("d")
                             .groupby(["ticker", "tod"])["cum_vol"]
                             .apply(lambda s: s.shift().expanding().mean()).reset_index(drop=True))
    res = res.merge(base[["ticker", "tod", "d", "avg_prior"]], on=["ticker", "tod", "d"], how="left")
    res["rvol_tod"] = res["volume"] / res["avg_prior"]

    res["ema6_vs_ema20_pct"] = (res["ema6"] / res["ema20"] - 1) * 100
    res["bullish_cross"] = res["ema6"] > res["ema20"]
    res["above_vwap_rt"] = res["close"] > res["vwap_rt"]
    res["pct_from_vwap_rt"] = (res["close"] / res["vwap_rt"] - 1) * 100

    keep = ["ticker", "d", "quote_utc", "close", "high", "low", "volume", "vol_delta",
            "ema6", "ema20", "ema6_vs_ema20_pct", "bullish_cross",
            "vwap_rt", "above_vwap_rt", "pct_from_vwap_rt", "rvol_tod"]
    res = res[keep].rename(columns={"d": "trade_date"})

    client.load_table_from_dataframe(
        res, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",   # rebuilt from full snapshot history each run
            time_partitioning=bigquery.TimePartitioning(field="quote_utc", type_="DAY"),
            clustering_fields=["ticker"],
        ),
    ).result()
    print(f"Done: {len(res)} realtime bars -> realtime_bars (EMA{EMA_FAST}/{EMA_SLOW} exact, VWAP approx).")


if __name__ == "__main__":
    main()
