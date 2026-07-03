"""
ascending_base.py — Standalone Ascending Base breakout (#11). DAILY, OHLCV LAYER A.
Rising swing (pivot) lows + break of a 40-bar range high. Grade = early Stage-2 (applied in the view,
from stage_engine): early-2A is the sweet spot (+0.32R vs mid +0.07). Stop = nearest MA within 3% (else LOD).

is_ascending_break = 40-bar-high breakout (first close above) AND rising pivot lows AND base not too wide.
Pivots are confirmed `pivot_order` bars AFTER the low (no look-ahead). NOTE (REV-C36): modest edge, still
below Flat Base; needs full-universe backtest + more filtering — deployed to GENERATE that evidence.

Requires: date, open, high, low, close, volume.
"""
import numpy as np
import pandas as pd


def _rising_lows(low, order, rise_factor):
    n = len(low)
    sl_idx = [j for j in range(order, n - order)
              if low[j] <= low[j - order:j].min() and low[j] <= low[j + 1:j + order + 1].min()]
    conf = {}
    for j in sl_idx:
        cb = j + order          # pivot becomes "known" order bars later
        if cb < n:
            conf[cb] = low[j]
    pl1 = np.full(n, np.nan); pl2 = np.full(n, np.nan)
    c1 = c2 = np.nan
    for i in range(n):
        if i in conf:
            c2 = c1; c1 = conf[i]
        pl1[i] = c1; pl2[i] = c2
    rising = (pl1 > pl2 * rise_factor)
    return rising, pl1, pl2


def run_ascending_base(df, pivot_order=4, rise_factor=1.02, range_len=40, max_width=0.6):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)
    n = len(d)

    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    e5 = d["close"].ewm(span=5, adjust=False).mean()
    e10 = d["close"].ewm(span=10, adjust=False).mean()
    e21 = d["close"].ewm(span=21, adjust=False).mean()
    s20 = d["close"].rolling(20).mean()
    s50 = d["close"].rolling(50).mean()

    rising, pl1, _ = _rising_lows(d["low"].values, pivot_order, rise_factor)
    d["rising_lows"] = rising

    rangehi = d["high"].rolling(range_len).max()
    brk = (d["close"] > rangehi.shift(1)) & (c1 <= rangehi.shift(2))
    base_low = d["low"].rolling(range_len).min()
    base_width = (rangehi - base_low) / base_low
    d["range_high"] = rangehi.shift(1)
    d["base_low"] = base_low
    d["base_width"] = base_width

    d["is_ascending_break"] = brk & rising & (base_width <= max_width) & (d["close"] > s50)

    # tight-MA stop: highest MA within 3% below close, else LOD
    C = d["close"].values; L = d["low"].values
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
