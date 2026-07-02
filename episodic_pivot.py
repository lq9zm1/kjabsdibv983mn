"""
episodic_pivot.py — Episodic Pivot (EP) OHLCV SCAN layer (Pradeep Bonde / PEAD).

⚠️ THIS IS LAYER A ONLY — the ARMED-candidate scan. Per REV-C16/C17, a real EP is
filtered by a FUNDAMENTAL/CATALYST layer B (earnings surprise+growth, float, analyst
coverage, institutional ownership, news, newsletter mentions) that OHLCV cannot see.
Pradeep buys <1% of scan hits. Layer B is DEFERRED to the refinement layer (EODHD
Fundamentals is a paid upcharge). So this scan is intentionally noisy; it marks
candidates + the OHLCV quality features that layer B will rank.

Pradeep's canonical scan (modern form):
    close/close[1] - 1 >= 0.04   (>= +4% close-up; NOT gap-required — EP is close-based, unlike BGU)
    volume >= 3 * avg50(volume, prior)   (>= 3x the 50-day avg vol as of the DAY BEFORE)
    volume >= 300,000                     (min liquidity)
    close >= 1

Requires columns: date, open, high, low, close, volume.

Pre-trigger (REV-C15): ARMED = scan bar. trigger_level = EP-day HIGH (daily-bar proxy
for the opening-range-high entry). stop = EP-day LOW. The triggered flag + pct_to_trigger
tracking live downstream (enrich view), same pattern as pocket_pivot.
"""

import numpy as np
import pandas as pd


def run_episodic_pivot(
    df,
    up_thresh=0.04,      # min close-up vs prior close (Pradeep +4%)
    vol_mult=3.0,        # volume >= vol_mult * avg50(prior)
    min_vol=300_000,     # min day volume (liquidity floor)
    min_price=1.0,       # min close
    avgv_len=50,         # avg-volume lookback (prior, excludes today)
    neglect_6mo=0.25,    # neglect proxy: prior 6mo return < this = "not already rallied"
    range_len=10,        # range-expansion comparison window
    hi_vol_len=252,      # window for "highest volume in ~1yr"
    price_floor_col=None,  # column for the min_price liquidity floor; BQ passes 'raw_close'
                           # (raw, unadjusted) so massively-split winners aren't wrongly cut.
                           # Falls back to 'close' if absent (e.g. local single-series CSVs).
):
    d = df.copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    floor_col = price_floor_col if (price_floor_col and price_floor_col in d.columns) else "close"

    c1 = d["close"].shift(1)
    # avg50 volume as of the PRIOR bar (Pradeep avgv50.1 — excludes today)
    avgv50_prior = d["volume"].shift(1).rolling(avgv_len).mean()

    d["pct_up"]   = d["close"] / c1 - 1.0
    d["gap_pct"]  = d["open"] / c1 - 1.0
    d["vol_ratio"] = d["volume"] / avgv50_prior

    # --- core Pradeep scan (the ARMED condition) ---
    d["ep_armed"] = (
        (d["pct_up"] >= up_thresh)
        & (d["volume"] >= vol_mult * avgv50_prior)
        & (d["volume"] >= min_vol)
        & (d[floor_col] >= min_price)
    )

    # --- OHLCV quality features (for layer-B ranking / grade) ---
    # highest volume in ~1yr (Pradeep: strong EPs print historic / multi-yr high vol)
    d["vol_252high"] = d["volume"] >= d["volume"].rolling(hi_vol_len, min_periods=20).max()
    # range expansion: today's range vs avg of prior N ranges (momentum-burst footprint)
    prior_rng = (d["high"] - d["low"]).shift(1).rolling(range_len).mean()
    d["range_exp"] = (d["high"] - d["low"]) / prior_rng
    # neglect proxy: return over prior ~6mo (ending yesterday); low = "not already rallied 3-6mo"
    d["prior_6mo_ret"] = c1 / d["close"].shift(127) - 1.0
    d["neglect_proxy"] = d["prior_6mo_ret"] < neglect_6mo
    # position in 52w range (0=at low, 1=at high); neglected EPs tend to fire from the lower band
    hi52 = d["high"].rolling(hi_vol_len, min_periods=20).max()
    lo52 = d["low"].rolling(hi_vol_len, min_periods=20).min()
    d["pos_52w"] = (d["close"] - lo52) / (hi52 - lo52)
    # IPO / young-name flag: no 6mo lookback yet -> neglect/pos features unreliable
    d["is_ipo_window"] = d["prior_6mo_ret"].isna()

    # --- pre-trigger (REV-C15) ---
    d["trigger_level"] = d["high"]   # EP-day high = daily-bar proxy for ORH entry
    d["stop"]          = d["low"]    # EP-day low = Pradeep stop

    return d


if __name__ == "__main__":
    import sys
    f = sys.argv[1]
    df = pd.read_csv(f)
    df = df.rename(columns={"vol_for_export": "volume"})
    df["date"] = pd.to_datetime(df["time"], unit="s")
    out = run_episodic_pivot(df)
    hits = out[out["ep_armed"]]
    print(f"{f}: {len(out)} bars, {len(hits)} ARMED ({len(hits)/len(out)*100:.1f}%)")
