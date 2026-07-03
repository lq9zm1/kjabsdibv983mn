"""
level_reclaim.py — Level Reclaim / UNR family (daily). OHLCV LAYER A.
Reclaim of a key MA after an undercut (undercut-and-rally), in an uptrend. The measured edge
(+0.4-0.5R, ~60% win, PF>2 across huge samples). Stop = undercut low.
Grade = gap-support-at-the-MA (validated in BQ post-deploy, the HUGE confluence) + RS-rising.
6/20 intraday trigger is R4.1, NOT here.

is_reclaim = close crosses back above a key MA after closing below it (recent undercut, <=25 bars):
  MAs checked largest-first -> which_ma = the most significant MA reclaimed.
  10/21 EMA · 20/50 SMA require close > 200SMA (uptrend). 200SMA reclaim = the trend-change (no filter, rare).
RS: pass 'spy_close'; rs_rising = rs > rs[rs_lookback] (positive lever for reclaims).
Requires: date, open, high, low, close, volume (+ optional spy_close).
"""
import numpy as np
import pandas as pd


def run_level_reclaim(df, rs_lookback=5, max_undercut=25, max_undercut_200=45):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)
    n = len(d)

    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    e10 = d["close"].ewm(span=10, adjust=False).mean()
    e21 = d["close"].ewm(span=21, adjust=False).mean()
    s20 = d["close"].rolling(20).mean()
    s50 = d["close"].rolling(50).mean()
    s200 = d["close"].rolling(200).mean()
    d["ext_atr_50"] = (d["close"] - s50) / atr

    if "spy_close" in d.columns:
        rs = d["close"] / pd.to_numeric(d["spy_close"], errors="coerce")
        d["rs_rising"] = rs > rs.shift(rs_lookback)
    else:
        d["rs_rising"] = np.nan

    C = d["close"].values; L = d["low"].values
    S200 = s200.values
    mas = [("50sma", s50.values), ("20sma", s20.values), ("21ema", e21.values), ("10ema", e10.values)]

    is_rec = np.zeros(n, bool)
    which = np.array([""] * n, dtype=object)
    und_low = np.full(n, np.nan); und_bars = np.full(n, np.nan)

    for i in range(200, n):
        # 200SMA reclaim (trend change, from below) — rare, no uptrend filter
        if C[i] > S200[i] and C[i - 1] <= S200[i - 1]:
            j = i - 1
            while j > 0 and C[j] <= S200[j]:
                j -= 1
            b = i - 1 - j
            if 1 <= b <= max_undercut_200:
                is_rec[i] = True; which[i] = "200sma"; und_bars[i] = b; und_low[i] = np.nanmin(L[j + 1:i + 1])
            continue
        if C[i] <= S200[i]:
            continue
        # uptrend: reclaim of a smaller MA (largest first)
        for name, M in mas:
            if C[i] > M[i] and C[i - 1] <= M[i - 1]:
                j = i - 1
                while j > 0 and C[j] <= M[j]:
                    j -= 1
                b = i - 1 - j
                if 1 <= b <= max_undercut:
                    is_rec[i] = True; which[i] = name; und_bars[i] = b; und_low[i] = np.nanmin(L[j + 1:i + 1])
                break

    d["is_reclaim"] = is_rec
    d["which_ma"] = which
    d["is_200ma_reclaim"] = (which == "200sma")
    d["undercut_low"] = und_low
    d["undercut_bars"] = und_bars
    d["stop"] = np.where(is_rec, und_low, np.nan)
    d["risk_pct"] = np.where(is_rec, (C / und_low - 1.0) * 100.0, np.nan)
    return d
