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
NOTE: EEMANi's Pine tests volume vs the max of ALL recent volume — wrong; real rule = max DOWN-day.

GRADE-CONFIRM FEATURES (carried in the table, NOT detection filters). The stage / RS-ratio /
RS>SPY parts of the gate need external data (stage_engine, SPX, Mansfield) and live in the
enrich view v_pocket_pivot_entries — only the OHLCV-derived confirms are materialized here:
  force_index = EMA(fi_len) of (Δclose × volume) [Elder, 13-EMA, matches TV] ; fi_positive = >0
  atr_pct     = EMA(14) of TrueRange / close * 100  (smoothed ATR %)
  vcp_tight   = atr_pct < atr_pct[20 bars ago] * 0.90   (volatility contracted >=10% vs 20d ago)
  Backtest (REV-C7/C9): FI>0 and vcp_tight both lift PP forward returns; used to grade, not remove.

df: columns date, open, high, low, close, volume (ascending). Returns a per-bar DataFrame.
"""
import numpy as np
import pandas as pd


def run_pocket_pivot(df, ma_len=10, lookback=10, offset=0.05,
                     require_upper_half=True, trend_ma=50, fi_len=13,
                     atr_len=14, vcp_lookback=20, vcp_ratio=0.90,
                     vdu_mult=0.5, vdu_lookback=5, avgv_len=50):
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
    down_vol_max = np.zeros(n)
    for i in range(n):
        lo = max(0, i - lookback)
        dv = v[lo:i][down_day[lo:i]]
        down_vol_max[i] = dv.max() if dv.size else 0.0

    with np.errstate(divide="ignore", invalid="ignore"):
        vol_ratio = np.where(down_vol_max > 0, v / down_vol_max, np.nan)
        dist_ma   = (c - sma_ma) / sma_ma

    near_ma    = np.abs(dist_ma) <= offset
    upper_half = c >= (h + l) / 2.0
    vol_ok     = v >= down_vol_max

    # Force Index (Elder) — 13-EMA of (Δclose × volume). Feature / grade confirm.
    raw_fi = np.zeros(n)
    raw_fi[1:] = (c[1:] - c[:-1]) * v[1:]
    force_index = pd.Series(raw_fi).ewm(span=fi_len, adjust=False).mean().to_numpy()
    fi_positive = force_index > 0

    # ATR% (smoothed) + VCP tightening — OHLCV-derived grade confirm.
    c_prev = np.empty(n); c_prev[0] = np.nan; c_prev[1:] = c[:-1]
    tr = np.maximum.reduce([h - l, np.abs(h - c_prev), np.abs(l - c_prev)])
    tr[0] = h[0] - l[0]
    atr = pd.Series(tr).ewm(span=atr_len, adjust=False).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        atr_pct = np.where(c > 0, atr / c * 100.0, np.nan)
    atr_pct_prev = pd.Series(atr_pct).shift(vcp_lookback).to_numpy()
    vcp_tight = atr_pct < (atr_pct_prev * vcp_ratio)

    # VDU (Volume Dry-Up / Kacher-Morales "VooDoo") precursor: recent_vdu = a dry-up day
    # (volume < vdu_mult * avg50) in the prior `vdu_lookback` days (excludes today). Validated
    # PP quality-booster ONLY combined with vcp_tight (REV-C25/C26): tight+VDU +0.75R vs
    # tight-no-VDU +0.26R (+0.49R lift); no effect on loose PPs. -> A-grade requirement.
    avgv = pd.Series(v).rolling(avgv_len).mean().to_numpy()
    vdu = v < (vdu_mult * avgv)
    recent_vdu = pd.Series(vdu).shift(1).rolling(vdu_lookback).max().fillna(0).astype(bool).to_numpy()

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
        "vol_ratio": np.round(vol_ratio, 2),
        "dist_ma_pct": np.round(dist_ma * 100, 2),
        "upper_half": upper_half,
        "force_index": np.round(force_index, 2),   # Elder Force Index (13-EMA)
        "fi_positive": fi_positive,                 # grade confirm: FI>0
        "atr_pct": np.round(atr_pct, 3),            # smoothed ATR %
        "vcp_tight": vcp_tight,                     # grade confirm: ATR% contracted >=10% vs 20d
        "recent_vdu": recent_vdu,                   # VDU dry-up in prior 5d (A-req WITH vcp_tight)
        "pocket_pivot": pp,
        "pp_type": np.where(pp, pp_type, ""),
    })
