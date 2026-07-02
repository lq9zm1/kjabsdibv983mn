"""
hvc.py (v2) — HVC (High-Volume Close) = the earnings/news "blue line". DAILY, OHLCV LAYER A. Setup #9.

is_hvc (refined, validated on 8 trader setups; ~0.5% of bars vs 2.2% before):
  vol_ratio >= vol_mult(3) x avg50  AND  gap-up  AND  green  AND  close top-40%  AND  close > SMA50.
  (Big-volume STRONG green GAP in an uptrend. Highest-vol-in-a-year "HV1" was too strict — killed
   DAVE/BROS/QBTS/ENPH/ANF; the gap+50SMA trend filter is the real de-noiser.)

GRADE = day-0 RS x follow-through (both validated; the strict def already baked in vol/stage/extension,
so those add nothing — only RS + the 20SMA follow-through discriminate):
  ft_confirmed = closed > SMA20 at +ft_check(2) bars   (the "follow-through day"; +9.4pp: +8.0% vs -1.4%)
  A  = rs_rising AND ft_confirmed
  B  = rs_rising OR  ft_confirmed
  C  = neither
  A?/B? = day-0 pending (recent HVC, follow-through not yet resolved -> resolves ~+2d).
Live follow-through tracker (no look-ahead): above_20, days_since_hvc, ft_hold. Line: above_hvc/pct_to_hvc.

RS: pass 'spy_close' (benchmark adj close by date) -> rs = close/spy_close, rs_rising = rs > rs[rs_lookback].
Requires: date, open, high, low, close, volume  (+ optional spy_close).
"""
import numpy as np
import pandas as pd


def run_hvc(df, vol_mult=3.0, avgv_len=50, min_close_pos=0.6, rs_lookback=5, ft_check=2, ft_window=5):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1 = d["close"].shift(1)
    n = len(d)

    d["vol_ratio"] = d["volume"] / d["volume"].rolling(avgv_len).mean()
    rng = (d["high"] - d["low"]).replace(0, np.nan)
    d["close_pos"] = (d["close"] - d["low"]) / rng
    d["gap"] = (d["open"] > c1)

    s10 = d["close"].rolling(10).mean()
    s20 = d["close"].rolling(20).mean()
    s50 = d["close"].rolling(50).mean()
    s200 = d["close"].rolling(200).mean()
    d["above_10"] = d["close"] > s10
    d["above_20"] = d["close"] > s20
    d["above_50"] = d["close"] > s50
    d["above_200"] = d["close"] > s200
    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=14, adjust=False).mean()
    d["ext_atr_50"] = (d["close"] - s50) / atr

    if "spy_close" in d.columns:
        rs = d["close"] / pd.to_numeric(d["spy_close"], errors="coerce")
        d["rs_rising"] = rs > rs.shift(rs_lookback)
    else:
        d["rs_rising"] = np.nan

    # refined HVC + line
    d["is_hvc"] = ((d["vol_ratio"] >= vol_mult) & d["gap"] & (d["close"] > d["open"])
                   & (d["close_pos"] >= min_close_pos) & (d["close"] > s50))
    d["is_gap_hvc"] = d["is_hvc"]  # gap is required now (kept for column parity)
    d["hvc_level"] = d["close"].where(d["is_hvc"]).ffill()
    d["hvc_date"] = d["date"].where(d["is_hvc"]).ffill()
    d["above_hvc"] = d["close"] > d["hvc_level"]
    d["pct_to_hvc"] = (d["close"] / d["hvc_level"] - 1.0) * 100.0

    # live follow-through tracker (no look-ahead)
    last_hvc = pd.Series(np.where(d["is_hvc"].values, np.arange(n), np.nan)).ffill()
    d["days_since_hvc"] = pd.Series(np.arange(n)) - last_hvc
    d["ft_hold"] = (d["days_since_hvc"].between(1, ft_window)) & d["above_20"]

    # follow-through confirmation for grading: held > SMA20 at +ft_check bars
    has_fwd = d["close"].shift(-ft_check).notna()
    ft_confirmed = (d["close"].shift(-ft_check) > s20.shift(-ft_check)) & has_fwd
    d["ft_confirmed"] = ft_confirmed.where(d["is_hvc"])
    pending = ~has_fwd

    # combined grade (day-0 RS x follow-through)
    rsr = d["rs_rising"].fillna(False)
    grade = np.select(
        [d["is_hvc"] & pending & rsr,
         d["is_hvc"] & pending & ~rsr,
         d["is_hvc"] & rsr & ft_confirmed,
         d["is_hvc"] & (rsr | ft_confirmed),
         d["is_hvc"]],
        ["A?", "B?", "A", "B", "C"],
        default="",
    )
    d["grade"] = grade
    return d
