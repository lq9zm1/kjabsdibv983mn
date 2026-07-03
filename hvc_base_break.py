"""
hvc_base_break.py — HVC-linked early base-break (#12, "1b"). DAILY, OHLCV LAYER A.
After an HVC (the blue line), price pulls back BELOW the line, forms a base with rising swing lows,
then breaks the 15-bar base high WHILE STILL UNDER the HVC line = entry a few days BEFORE #10's reclaim.
Grade = early Stage-2 (applied in the view). Stop = nearest MA within 3% (else LOD).

is_1b_break = active HVC (4..60 bars) AND price dipped below line AND rising pivot lows
              AND break of 15-bar base high AND close < hvc_level (early, under the line).
Elite but RARE exemplar edge (+3.84R early-2A, n=19) — deployed to get the full-universe verdict (REV-C36).
Requires: date, open, high, low, close, volume.
"""
import numpy as np
import pandas as pd


def _rising_lows(low, order, rise_factor):
    n = len(low)
    sl = [j for j in range(order, n - order)
          if low[j] <= low[j - order:j].min() and low[j] <= low[j + 1:j + order + 1].min()]
    conf = {}
    for j in sl:
        cb = j + order
        if cb < n:
            conf[cb] = low[j]
    pl1 = np.full(n, np.nan); pl2 = np.full(n, np.nan); c1 = c2 = np.nan
    for i in range(n):
        if i in conf:
            c2 = c1; c1 = conf[i]
        pl1[i] = c1; pl2[i] = c2
    return pl1 > pl2 * rise_factor


def run_hvc_base_break(df, vol_mult=3.0, avgv_len=50, min_close_pos=0.6,
                       pivot_order=4, rise_factor=1.02, base_len=15, min_days=4, max_days=60):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)
    n = len(d)

    vr = d["volume"] / d["volume"].rolling(avgv_len).mean()
    cp = (d["close"] - d["low"]) / (d["high"] - d["low"]).replace(0, np.nan)
    s50 = d["close"].rolling(50).mean()
    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    e5 = d["close"].ewm(span=5, adjust=False).mean()
    e10 = d["close"].ewm(span=10, adjust=False).mean()
    e21 = d["close"].ewm(span=21, adjust=False).mean()
    s20 = d["close"].rolling(20).mean()

    # HVC line (same as #9/#10)
    ishvc = ((vr >= vol_mult) & (d["open"] > c1) & (d["close"] > d["open"])
             & (cp >= min_close_pos) & (d["close"] > s50)).values
    line = pd.Series(np.where(ishvc, d["close"].values, np.nan)).ffill()
    lastH = pd.Series(np.where(ishvc, np.arange(n), np.nan)).ffill()
    d["hvc_level"] = line.values

    rising = _rising_lows(d["low"].values, pivot_order, rise_factor)
    d["rising_lows"] = rising
    basehi = d["high"].rolling(base_len).max()

    C = d["close"].values; L = d["low"].values
    is_1b = np.zeros(n, bool); dsince = np.full(n, np.nan); pdepth = np.full(n, np.nan)
    for i in range(1, n):
        if np.isnan(line.iloc[i]) or np.isnan(lastH.iloc[i]):
            continue
        hb = int(lastH.iloc[i]); days = i - hb
        if days < min_days or days > max_days:
            continue
        if not (C[i] > basehi.iloc[i - 1] and C[i - 1] <= basehi.iloc[i - 2]):   # base-break (15-bar high)
            continue
        if C[i] >= line.iloc[i]:                                                 # EARLY: still under the line
            continue
        if np.nanmin(C[hb:i]) >= line.iloc[i]:                                   # a real dip below happened
            continue
        if not bool(rising[i]):                                                  # rising pivot lows
            continue
        is_1b[i] = True; dsince[i] = days
        pdepth[i] = (line.iloc[i] - np.nanmin(L[hb:i])) / line.iloc[i] * 100.0
    d["is_1b_break"] = is_1b
    d["days_since_hvc"] = dsince
    d["pullback_depth"] = pdepth

    # tight-MA stop
    E5 = e5.values; E10 = e10.values; E21 = e21.values; S20 = s20.values
    stop = np.empty(n)
    for i in range(n):
        cands = [m[i] for m in (E5, E10, E21, S20) if m[i] < C[i] and m[i] >= C[i] * 0.97]
        stop[i] = max(cands) if cands else L[i]
    d["tight_ma_stop"] = stop
    d["stop"] = stop
    d["risk_pct"] = (d["close"] / stop - 1.0) * 100.0
    d["ext_atr_50"] = (d["close"] - s50) / atr
    return d
