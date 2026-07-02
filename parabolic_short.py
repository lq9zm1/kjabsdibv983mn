"""
parabolic_short.py — Parabolic Short (Qullamaggie). SHORT, OHLCV LAYER A.
DAILY flag only — the actual execution is INTRADAY (R4.1); parabolic short is essentially a day trade.

⚠️ Reconstructed from v8 + refined this session. DIFF vs the v8 chat before trusting blindly.

State (ARMED = short watchlist):
  ext_atr_50 = (close-SMA50)/ATR(14) >= 7  OR  run_pct(20d) >= 100%   (run% catches violent
  small-caps where ATR explodes and self-dampens ext to ~6 — CAR/OPEN/PLUG/GME).
Entry (SHORT) = first red day (close<prior close) after an ARMED bar. Stop = 5-day high +2%.

GRADE v-final (routing-fixed; R-expectancy on 11 exemplars — RELATIVE, re-confirm full universe):
  peaks over trailing peak_lb bars at the entry bar.
  dailyA   = ext_peak>=9 AND rsi_peak>=76 AND stretch_peak<5
  A+       = dailyA AND gap_peak>=3                                   (elite; +1.12R)
  A        = dailyA                                                   (daily short; +0.19R)
  INTRADAY = rsi_peak>=76 AND (stretch_peak>=5 OR run_peak>=100%) AND NOT dailyA
             (violent blow-off incl ext-dampened squeezes — daily-R ~0, short INTRADAY not daily)
  C        = else                                                    (skip)

Day-of (intraday-prep) cols: prior_day_low/high, first_break_prior_low (Breitstein backside),
  gap_up_fade. Pre-anticipation (imminence): pct_to_prior_low (->0 = trigger imminent),
  pct_to_prior_high, pct_from_peak, pct_vs_avwap, below_avwap.
🚨 avwap = DAILY PROXY (anchored at last cross above SMA50), NOT session VWAP — true VWAP/ORB = R4.1.

Requires: date, open, high, low, close, volume.
"""
import numpy as np
import pandas as pd


def _wilder_rsi(close, n=14):
    d = close.diff()
    ag = d.clip(lower=0).ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    al = (-d.clip(upper=0)).ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    return 100.0 - 100.0 / (1.0 + ag / al)


def _consec_run(mask):
    m = mask.astype(int)
    return (m * (m.groupby((m != m.shift()).cumsum()).cumcount() + 1)).astype(int)


def run_parabolic_short(df, atr_len=14, sma_len=50, ext_thresh=7.0, run_len=20, run_thresh=1.0,
                        high_len=5, peak_lb=6, rsi_len=14, stretch_low_len=5, bb_len=20,
                        grade_ext=9.0, grade_rsi=76.0, grade_stretch=5.0, grade_gap=3):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    c1, h1, l1 = d["close"].shift(1), d["high"].shift(1), d["low"].shift(1)

    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=atr_len, adjust=False).mean()
    sma50 = d["close"].rolling(sma_len).mean()
    d["ext_atr_50"] = (d["close"] - sma50) / atr
    d["pct_above_50"] = (d["close"] / sma50 - 1.0) * 100.0
    d["up_streak"] = _consec_run(d["close"] > c1)
    d["run_pct"] = d["close"] / d["close"].shift(run_len) - 1.0

    armed = (d["ext_atr_50"] >= ext_thresh) | (d["run_pct"] >= run_thresh)
    d["armed"] = armed
    d["parabolic_short"] = armed.shift(1).fillna(False) & (d["close"] < c1)

    d["rsi"] = _wilder_rsi(d["close"], rsi_len)
    d["stretch5"] = (d["close"] - d["low"].rolling(stretch_low_len).min()) / atr
    mid = d["close"].rolling(bb_len).mean()
    sd = d["close"].rolling(bb_len).std(ddof=0)
    d["bb_out"] = d["close"] > (mid + 2.0 * sd)
    d["gap_run"] = _consec_run(d["open"] > c1)

    rng = (d["high"] - d["low"]).replace(0, np.nan)
    d["bearish_candle"] = (d["close"] < d["open"]) & (((d["close"] - d["low"]) / rng) < 0.4) & \
                          (((d["high"] - d[["close", "open"]].max(axis=1)) / rng) > 0.3)

    d["ext_peak"] = d["ext_atr_50"].rolling(peak_lb, min_periods=1).max()
    d["rsi_peak"] = d["rsi"].rolling(peak_lb, min_periods=1).max()
    d["stretch_peak"] = d["stretch5"].rolling(peak_lb, min_periods=1).max()
    d["gap_peak"] = d["gap_run"].rolling(peak_lb, min_periods=1).max()
    d["run_peak"] = d["run_pct"].rolling(peak_lb, min_periods=1).max()

    # anchored VWAP (DAILY PROXY) — resets at each cross above SMA50 (the run launch)
    grp = ((d["close"] > sma50) & (c1 <= sma50.shift(1))).cumsum()
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    vol = d["volume"].fillna(0)
    d["avwap"] = (tp * vol).groupby(grp).cumsum() / vol.groupby(grp).cumsum().replace(0, np.nan)
    run_high = d["high"].groupby(grp).cummax()

    # day-of (intraday-prep)
    d["prior_day_low"] = l1
    d["prior_day_high"] = h1
    d["gap_up_fade"] = (d["open"] > c1) & (d["close"] < d["open"])
    seg = (armed != armed.shift()).cumsum()
    bk = (d["low"] < l1) & armed
    d["first_break_prior_low"] = bk & (bk.groupby(seg).cumsum() == 1)

    # pre-anticipation (imminence)
    d["pct_to_prior_low"] = (d["close"] / l1 - 1.0) * 100.0
    d["pct_to_prior_high"] = (d["close"] / h1 - 1.0) * 100.0
    d["pct_from_peak"] = (d["close"] / run_high - 1.0) * 100.0
    d["pct_vs_avwap"] = (d["close"] / d["avwap"] - 1.0) * 100.0
    d["below_avwap"] = d["close"] < d["avwap"]

    d["stop"] = d["high"].rolling(high_len, min_periods=1).max() * 1.02

    daily_a = (d["ext_peak"] >= grade_ext) & (d["rsi_peak"] >= grade_rsi) & (d["stretch_peak"] < grade_stretch)
    violent = (d["rsi_peak"] >= grade_rsi) & ((d["stretch_peak"] >= grade_stretch) | (d["run_peak"] >= run_thresh))
    g = np.where(daily_a & (d["gap_peak"] >= grade_gap), "A+",
                 np.where(daily_a, "A", np.where(violent, "INTRADAY", "C")))
    d["grade"] = np.where(d["parabolic_short"], g, "")
    return d
