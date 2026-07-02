"""
buyable_gap_up.py — Buyable Gap-Up (BGU) OHLCV SCAN (Gil Morales & Chris Kacher,
"In the Trading Cockpit with the O'Neil Disciples"). ⚠️ LAYER A only.

Detection (2 hard Morales rules):
    gap (open - close[1]) >= 0.75 * ATR(40)[prior]     (volatility-adaptive gap magnitude)
    volume >= 1.5 * SMA(volume, 50)                     (150% of 50d avg)

BGU = a CONSTRUCTIVE / leading-stock gap (base / uptrend) — the OPPOSITE of EP's neglect.
The "fundamentally sound / catalyst" part is a fundamental/news layer -> refinement (REV-C21).

Quality (for the grade): close>=open (bullish hold; closing near lows = warning) ·
low>high[1] (unfilled rising window = accumulation not distribution) · constructive
(price>SMA50[>SMA200]). Pre-trigger (REV-C15): entry_ref = gap-day open (Morales buys at
open); stop = gap-day intra-day low (~2% porosity). Requires: date,open,high,low,close,volume.
"""
import numpy as np
import pandas as pd


def run_buyable_gap_up(
    df,
    atr_len=40,          # Morales ATR length
    gap_atr_mult=0.75,   # gap >= this * ATR(40)[prior]
    vol_len=50,          # avg-volume length
    vol_mult=1.5,        # volume >= this * avg50
    min_price=1.0,
    sma_ctx=50,          # constructive context MA
    sma_ctx2=200,
    fi_len=13,           # Force Index EMA length (Elder) — grade signal (A requires FI>0)
    require_unfilled=False,  # if True, also require low>high[1] (rising window) to ARM
    price_floor_col=None,    # BQ passes 'raw_close' for the min_price floor
):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    floor_col = price_floor_col if (price_floor_col and price_floor_col in d.columns) else "close"

    c1 = d["close"].shift(1)
    h1 = d["high"].shift(1)
    # ATR(40) as SIMPLE mean of True Range (Morales/ThinkScript convention), of the PRIOR bar
    tr = pd.concat([d["high"] - d["low"], (d["high"] - c1).abs(), (d["low"] - c1).abs()], axis=1).max(axis=1)
    atr_prior = tr.rolling(atr_len).mean().shift(1)
    avgvol = d["volume"].rolling(vol_len).mean()

    d["gap_pts"]   = d["open"] - c1
    d["gap_atr"]   = d["gap_pts"] / atr_prior
    d["vol_ratio"] = d["volume"] / avgvol
    d["unfilled"]  = d["low"] > h1                    # rising window (gap not filled)

    armed = (
        (d["gap_atr"] >= gap_atr_mult)
        & (d["volume"] >= vol_mult * avgvol)
        & (d[floor_col] >= min_price)
    )
    if require_unfilled:
        armed = armed & d["unfilled"]
    d["bgu_armed"] = armed

    # quality / context features
    d["bullish_hold"] = d["close"] >= d["open"]       # not closing near lows
    s50  = d["close"].rolling(sma_ctx).mean()
    s200 = d["close"].rolling(sma_ctx2).mean()
    d["constructive"] = d["close"] > s50              # in/above uptrend
    d["above_200"]    = d["close"] > s200
    d["bgu_type"]     = np.where(d["constructive"], "standard", "bottom_fishing")
    # Force Index (Elder) — grade signal. Light backtest (REV-C22): FI>0 BGU +1.48R/PF4.51 vs FI<0 -0.12.
    d["force_index"]  = (d["close"].diff() * d["volume"]).ewm(span=fi_len, adjust=False).mean()
    d["fi_positive"]  = d["force_index"] > 0

    # pre-trigger (same-day gap entry)
    d["entry_ref"] = d["open"]                         # Morales buys at the open
    d["stop"]      = d["low"] * 0.98                   # ~2% below the gap-day low (porosity)
    return d


if __name__ == "__main__":
    import sys
    df = pd.read_csv(sys.argv[1]).rename(columns={"vol_for_export": "volume"})
    df["date"] = pd.to_datetime(df["time"], unit="s")
    out = run_buyable_gap_up(df)
    h = out[out["bgu_armed"]]
    print(f"{sys.argv[1]}: {len(out)} bars, {len(h)} BGU ({len(h)/len(out)*100:.1f}%)")
