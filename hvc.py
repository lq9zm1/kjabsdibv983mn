"""
hvc.py — HVC (High-Volume Close) detector = the earnings/news "blue line". DAILY, OHLCV LAYER A.
The HVC's close = a reference level (institutions stepped in). Setup #9. Higher-Low (#10) reclaims it.

is_hvc  = volume >= vol_mult x avg50  AND  green (close>open)  AND  close in top (1-min_close_pos) of range.
          (A big-volume STRONG close. A high-volume RED close = earnings FAIL -> NOT hvc. Validated:
           DOCN 5/5/26 = hvc; MU +52% beats that sold off = rejected.)
is_gap_hvc = is_hvc AND gap-up (the earnings-gap subset; else = follow-through / day-after).
hvc_level = the HVC close, carried forward. above_hvc / pct_to_hvc = the "above=good, below=bad" signal.

GRADE (at-bar, no look-ahead; validated on followthrough +3d->+13d — RELATIVE, re-confirm full universe):
  Predictors of followthrough = MA-trend + RS (NOT volume; vol marks the event, +0.0pp on followthrough).
  A+ = rs_rising AND close>SMA50 AND gap        (mean +4.5% / 55%)
  A  = rs_rising AND close>SMA50                 (mean +4.2% / 55%)
  B  = rs_rising OR  close>SMA50                 (+1.7%)
  C  = neither                                   (-0.7%)
Context cols (not grade): above_10/20/200, ext_atr_50 (extended is fine/good for a long HVC).

RS: pass a 'spy_close' column (benchmark adj close, aligned by date) -> rs = close/spy_close,
    rs_rising = rs > rs[rs_lookback]. Without spy_close, rs_rising = NaN (grade degrades to MA-only).

Requires: date, open, high, low, close, volume  (+ optional spy_close).
"""
import numpy as np
import pandas as pd


def run_hvc(df, vol_mult=2.0, avgv_len=50, min_close_pos=0.6, rs_lookback=5):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)

    avgv = d["volume"].rolling(avgv_len).mean()
    d["vol_ratio"] = d["volume"] / avgv
    rng = (d["high"] - d["low"]).replace(0, np.nan)
    d["close_pos"] = (d["close"] - d["low"]) / rng
    d["gap"] = (d["open"] > c1)

    s10 = d["close"].rolling(10).mean()
    s20 = d["close"].rolling(20).mean()
    s50 = d["close"].rolling(50).mean()
    s200 = d["close"].rolling(200).mean()
    d["sma_50"] = s50
    d["above_10"] = d["close"] > s10
    d["above_20"] = d["close"] > s20
    d["above_50"] = d["close"] > s50
    d["above_200"] = d["close"] > s200
    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    d["ext_atr_50"] = (d["close"] - s50) / atr

    # relative strength vs benchmark (if provided)
    if "spy_close" in d.columns:
        rs = d["close"] / pd.to_numeric(d["spy_close"], errors="coerce")
        d["rs_ratio"] = rs
        d["rs_rising"] = rs > rs.shift(rs_lookback)
    else:
        d["rs_ratio"] = np.nan
        d["rs_rising"] = np.nan

    # the HVC event + line
    d["is_hvc"] = (d["vol_ratio"] >= vol_mult) & (d["close"] > d["open"]) & (d["close_pos"] >= min_close_pos)
    d["is_gap_hvc"] = d["is_hvc"] & d["gap"]
    d["hvc_level"] = d["close"].where(d["is_hvc"]).ffill()
    d["hvc_date"] = d["date"].where(d["is_hvc"]).ffill()
    d["above_hvc"] = d["close"] > d["hvc_level"]
    d["pct_to_hvc"] = (d["close"] / d["hvc_level"] - 1.0) * 100.0

    # grade (only on HVC bars) — MA-trend + RS
    rsr = d["rs_rising"].fillna(False)
    a = rsr & d["above_50"]
    g = np.where(a & d["gap"], "A+", np.where(a, "A", np.where(rsr | d["above_50"], "B", "C")))
    d["grade"] = np.where(d["is_hvc"], g, "")
    return d
