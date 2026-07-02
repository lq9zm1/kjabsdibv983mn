"""
higher_low.py — Higher-Low reclaim of the HVC line (#10). Consumes the HVC "blue line".
Option-1 (MA-hold): after an HVC, a pullback that HELD above the 50SMA -> close RECLAIMS the line = entry.
Stop = low of the reclaim day. Ideal entry timing (day-before 6/20 / VWAP / rising-flat RS) = intraday (R4.1).

GRADE (validated; mean-reversion snapback = OPPOSITE of the HVC day-0 grade):
  A = NOT extended (ext_atr_50 < 3) AND RS-not-rising   (meanR +5.12, PF 10.3)
  B = one of them                                        (+0.84)
  C = neither (extended + RS-rising = late)              (+0.43)
RS: pass 'spy_close' (benchmark adj close by date); rs_not_rising = rs <= rs[rs_lookback].
Requires: date, open, high, low, close, volume  (+ optional spy_close).
"""
import numpy as np
import pandas as pd


def run_higher_low(df, vol_mult=3.0, avgv_len=50, min_close_pos=0.6, rs_lookback=5,
                   min_days=4, max_days=60, grade_ext=3.0):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)
    n = len(d)

    vr = d["volume"] / d["volume"].rolling(avgv_len).mean()
    cp = (d["close"] - d["low"]) / (d["high"] - d["low"]).replace(0, np.nan)
    s10 = d["close"].rolling(10).mean()
    s50 = d["close"].rolling(50).mean()
    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    d["ext_atr_50"] = (d["close"] - s50) / atr
    d["sma_10"] = s10

    # HVC line (same as #9)
    ishvc = ((vr >= vol_mult) & (d["open"] > c1) & (d["close"] > d["open"])
             & (cp >= min_close_pos) & (d["close"] > s50)).values
    line = pd.Series(np.where(ishvc, d["close"].values, np.nan)).ffill()
    lastH = pd.Series(np.where(ishvc, np.arange(n), np.nan)).ffill()
    d["hvc_level"] = line.values

    # RS
    if "spy_close" in d.columns:
        rs = d["close"] / pd.to_numeric(d["spy_close"], errors="coerce")
        d["rs_not_rising"] = rs <= rs.shift(rs_lookback)
    else:
        d["rs_not_rising"] = np.nan

    # reclaim detection (option-1 MA-hold)
    C = d["close"].values; L = d["low"].values; S50 = s50.values
    is_rec = np.zeros(n, bool); dsince = np.full(n, np.nan); plow = np.full(n, np.nan); pdepth = np.full(n, np.nan)
    for i in range(1, n):
        if np.isnan(line.iloc[i]) or np.isnan(lastH.iloc[i]):
            continue
        hb = int(lastH.iloc[i]); days = i - hb
        if days < min_days or days > max_days:
            continue
        if not (C[i] > line.iloc[i] and C[i - 1] <= line.iloc[i]):   # cross back above the line
            continue
        if np.nanmin(C[hb:i]) >= line.iloc[i]:                        # require a real dip below the line
            continue
        j = hb + int(np.nanargmin(L[hb:i]))                           # pullback low bar
        if L[j] < S50[j]:                                             # MA-hold: pullback low held above 50SMA
            continue
        is_rec[i] = True; dsince[i] = days; plow[i] = L[j]; pdepth[i] = (line.iloc[i] - L[j]) / line.iloc[i] * 100.0
    d["is_reclaim"] = is_rec
    d["days_since_hvc"] = dsince
    d["pullback_low"] = plow
    d["pullback_depth"] = pdepth
    d["stop"] = d["low"]                                              # stop = low of the reclaim day
    d["risk_pct"] = (d["close"] / d["low"] - 1.0) * 100.0

    # grade on reclaim bars
    ext_lo = d["ext_atr_50"] < grade_ext
    rsn = d["rs_not_rising"].fillna(False)
    A = ext_lo & rsn
    B = (ext_lo | rsn) & ~A
    g = np.where(d["is_reclaim"] & A, "A", np.where(d["is_reclaim"] & B, "B", np.where(d["is_reclaim"], "C", "")))
    d["hl_grade"] = g
    return d
