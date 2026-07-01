"""
Pocket Pivot detector — Gil Morales & Chris Kacher ("Trade Like an O'Neil Disciple").

A pocket pivot = an UP day (close > prior close) where today's volume EXCEEDS the
highest DOWN-day volume of the prior `lookback` days, with the close NEAR the 10-day MA
(not extended) and closing in the UPPER HALF of its range. It marks institutional
accumulation INSIDE a base — an early entry before the visible breakout.

Variant (classification, not a filter):
  - roundabout     : close above the trend MA (50-day) — rounding out the right side of a base
  - bottom_fishing : close below the trend MA — earlier/riskier, coming up off the lows

Canonical rule (reconciled across Morales webinar + Chartmill + EEMANi):
  up_day AND volume >= max(down-day volume over prior `lookback` days)
         AND |close - SMA(ma_len)| / SMA(ma_len) <= offset       (near the 10-day, not extended)
         AND close >= (high+low)/2                                (upper half of range)

NOTE: EEMANi's Pine tests volume vs the max of ALL recent volume — that's wrong; the real
rule is vs the max DOWN-day volume. This uses the correct definition.

FORCE INDEX (Elder), carried as a FEATURE / grade confirm — NOT a detection filter:
  force_index = EMA(fi_len) of (close - prior close) * volume   [13-EMA smoothed, matches TV]
  fi_positive = force_index > 0
  Backtest (REV-C7): PPs with fi_positive underperform far less often — FI<0 PPs averaged
  ~-0.4%/10d vs +1.5% for FI>0 (robust 5/6 tickers). Used to DOWNGRADE FI<0 pivots in the
  grade, not to remove them.

df: columns date, open, high, low, close, volume (ascending). Returns a per-bar DataFrame.
"""
import numpy as np
import pandas as pd


def run_pocket_pivot(df, ma_len=10, lookback=10, offset=0.05,
                     require_upper_half=True, trend_ma=50, fi_len=13):
    d = df.reset_index(drop=True).copy()
    n = len(d)
    c = d["close"].to_numpy(float)
    h = d["high"].to_numpy(float)
    l = d["low"].to_numpy(float)
    v = d["volume"].to_numpy(float)

    sma_ma  = d["close"].rolling(ma_len).mean().to_numpy()
    sma_trd = d["close"].rolling(trend_ma).mean().to_numpy()

    up_day = np.zeros(n, bool);   up_day[1:]   = c[1:] > c[:-1]
    down_day = np.zeros(n, bool); down_day[1:] = c[1:] < c[:-1]

    # highest DOWN-day volume over the prior `lookback` days (excludes today).
    # no down day in the window -> 0 (up-day volume vacuously exceeds it; the near-MA
    # filter still excludes the extended "10 up days in a row" case).
    down_vol_max = np.zeros(n)
    for i in range(n):
        lo = max(0, i - lookback)
        seg_v = v[lo:i]
        seg_d = down_day[lo:i]
        dv = seg_v[seg_d]
        down_vol_max[i] = dv.max() if dv.size else 0.0

    with np.errstate(divide="ignore", invalid="ignore"):
        vol_ratio = np.where(down_vol_max > 0, v / down_vol_max, np.nan)
        dist_ma   = (c - sma_ma) / sma_ma

    near_ma    = np.abs(dist_ma) <= offset
    upper_half = c >= (h + l) / 2.0
    vol_ok     = v >= down_vol_max

    # Force Index (Elder) — 13-EMA of (Δclose × volume). Feature / grade confirm only.
    raw_fi = np.zeros(n)
    raw_fi[1:] = (c[1:] - c[:-1]) * v[1:]
    force_index = pd.Series(raw_fi).ewm(span=fi_len, adjust=False).mean().to_numpy()
    fi_positive = force_index > 0

    pp = up_day & vol_ok & near_ma & ~np.isnan(sma_ma)
    if require_upper_half:
        pp = pp & upper_half

    pp_type = np.where(np.isnan(sma_trd), "",
                       np.where(c > sma_trd, "roundabout", "bottom_fishing"))

    return pd.DataFrame({
        "date": d["date"],
        "close": c,
        "volume": v,
        "sma10": np.round(sma_ma, 4),
        "sma50": np.round(sma_trd, 4),
        "up_day": up_day,
        "down_vol_max": down_vol_max,
        "vol_ratio": np.round(vol_ratio, 2),      # today's vol / biggest down-day vol
        "dist_ma_pct": np.round(dist_ma * 100, 2),
        "upper_half": upper_half,
        "force_index": np.round(force_index, 2),  # Elder Force Index (13-EMA smoothed)
        "fi_positive": fi_positive,               # grade confirm: FI>0
        "pocket_pivot": pp,
        "pp_type": np.where(pp, pp_type, ""),
    })
