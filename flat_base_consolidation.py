"""
Unified Consolidation / Base Detector  (Approach B)
===================================================
Catches the FULL spectrum of continuation consolidations that precede a breakout:
    flag  ->  short base  ->  flat base  ->  long base   (and triangle/wedge/pennant shapes)

Design decisions (locked with Alex 2026-07-01):
  * ONE detector, low min_len (=5), catch broad, then classify by LENGTH + label SHAPE;
    grading (liquidity/ADR/mktcap/momentum) is a DOWNSTREAM layer, NOT here.
  * PULLBACK-ANCHORED to fix the reset-on-new-high bug: a consolidation is only
    established AFTER price pulls back >= pullback_min from a swing high; the swing high
    becomes a STICKY resistance top (does not reset on minor new highs inside the range).
  * Causal / point-in-time: every decision at bar i uses only data through bar i.
  * Emits TWO entry levels: pivot_entry = top (breakout), cheat_entry = low (Minervini Low Cheat).
  * Shape is labelled from ATR-normalized slopes of the highs/lows inside the window.
    (Breakout level = horizontal top for now; sloped-resistance triangles are a later refinement.)

State machine per ticker (single pass):
  SEEKING (no active consol):
     - track running swing high `rhigh`; when price pulls back >= pullback_min from it AND
       there was a prior advance (>= prior_leg over prior_lb) -> ESTABLISH consol at rhigh.
  IN-CONSOL:
     - low  = running min since start;  depth = (top-low)/top
     - close > top  & len >= min_len  -> TRIGGERED (breakout). emit, reset.
     - depth > max_depth OR len > max_len -> FAIL (too deep/long). emit unbroken, reset.
     - else in range: state = ARMED if len >= min_len else WATCH.

run_flat_base_consolidation(df) -> per-bar DataFrame.
consol_episodes(h)    -> one row per consolidation (start..end).
"""
import numpy as np
import pandas as pd


# ------------------------------------------------------------------ helpers
def _atr(high, low, close, n=14):
    pc = np.empty_like(close); pc[0] = close[0]; pc[1:] = close[:-1]
    tr = np.maximum(high - low, np.maximum(np.abs(high - pc), np.abs(low - pc)))
    a = np.full_like(close, np.nan)
    if len(close) >= n:
        a[n - 1] = tr[:n].mean()
        for i in range(n, len(close)):
            a[i] = (a[i - 1] * (n - 1) + tr[i]) / n
    return a


def _len_class(L, flag_max, short_max, flat_max):
    if L <= flag_max:  return "flag"
    if L <= short_max: return "short_base"
    if L <= flat_max:  return "flat_base"
    return "long_base"


def _shape(highs, lows, atr, flat_th=0.06, conv_th=0.85):
    """Classify a consolidation window from ATR-normalized slopes of highs & lows.
    Slopes in ATR/bar. flat_th ~ how flat counts as flat. conv_th ~ endGap/startGap to call it converging."""
    L = len(highs)
    if L < 3 or not np.isfinite(atr) or atr <= 0:
        return "forming"
    x = np.arange(L)
    su = np.polyfit(x, highs, 1)[0] / atr      # upper slope (ATR/bar)
    sl = np.polyfit(x, lows,  1)[0] / atr      # lower slope
    start_gap = highs[0] - lows[0]
    end_gap = highs[-1] - lows[-1]
    converging = end_gap < start_gap * conv_th and start_gap > 0
    up_flat, dn_flat = abs(su) < flat_th, abs(sl) < flat_th
    up_rise, up_fall = su >= flat_th, su <= -flat_th
    dn_rise, dn_fall = sl >= flat_th, sl <= -flat_th

    if up_flat and dn_flat:            return "rectangle"          # flat base
    if up_flat and dn_rise:            return "asc_triangle"
    if up_fall and dn_flat:            return "desc_triangle"
    if up_fall and dn_rise:            return "sym_triangle"       # (pennant if short)
    if up_rise and dn_rise and converging: return "rising_wedge"
    if up_fall and dn_fall and converging: return "falling_wedge"
    if up_fall and dn_fall:            return "bull_flag"          # parallel down-drift after pole
    if up_rise and dn_rise:            return "channel_up"
    return "channel"


# ------------------------------------------------------------------ main
def run_flat_base_consolidation(df,
                      min_len=5, max_len=325, max_depth=0.35,
                      res_lb=25, prior_lb=103, prior_leg=0.20,
                      pole_lb=20,
                      flag_max=14, short_max=24, flat_max=64):
    d = df.reset_index(drop=True)
    high = d["high"].to_numpy(float); low = d["low"].to_numpy(float); close = d["close"].to_numpy(float)
    n = len(d)
    atr = _atr(high, low, close, 14)

    in_c   = np.zeros(n, bool)
    state  = np.array([""] * n, object)
    a_top  = np.full(n, np.nan); a_low = np.full(n, np.nan); a_dep = np.full(n, np.nan)
    a_len  = np.zeros(n, int);   a_lc  = np.array([""] * n, object)
    a_shp  = np.array([""] * n, object); a_pole = np.full(n, np.nan)
    brk    = np.zeros(n, bool)

    start = -1; top = np.nan; pole = np.nan

    for i in range(n):
        # ---- SEEKING: anchor a base under a RECENT rolling resistance (local peak), not the all-time max.
        #      A base begins the first bar price comes OFF a recent peak (high[i] < peak), after a prior advance.
        if start < 0 and i >= res_lb:
            w0 = i - res_lb
            peak_rel = int(np.argmax(high[w0:i + 1]))
            peak_bar = w0 + peak_rel
            peak = high[peak_bar]
            if high[i] < peak and peak_bar < i:                      # price has pulled off the recent peak
                blp = low[max(0, peak_bar - prior_lb):peak_bar + 1].min()
                if peak >= blp * (1 + prior_leg):                    # prior advance into the peak
                    start = peak_bar; top = peak
                    pl_lo = low[max(0, peak_bar - pole_lb):peak_bar + 1].min()
                    pole = (top - pl_lo) / pl_lo if pl_lo > 0 else np.nan

        # ---- IN-CONSOL
        if start >= 0:
            L = i - start
            blow = low[start:i + 1].min()
            depth = (top - blow) / top
            if close[i] > top:                                       # closed above resistance
                if L >= min_len:                                     # -> BREAKOUT
                    brk[i] = True; state[i] = "TRIGGERED"
                    in_c[i] = True; a_top[i] = top; a_low[i] = blow; a_dep[i] = depth
                    a_len[i] = L; a_lc[i] = _len_class(L, flag_max, short_max, flat_max)
                    a_shp[i] = _shape(high[start:i + 1], low[start:i + 1], atr[i]); a_pole[i] = pole
                start = -1; continue                                 # (too-young base just resolves upward, no signal)
            if depth > max_depth or L > max_len:                     # FAIL (too deep / too long)
                start = -1; continue
            # upper wicks that DON'T close above top keep a flat-top base intact
            in_c[i] = True; a_top[i] = top; a_low[i] = blow; a_dep[i] = depth
            a_len[i] = L; a_lc[i] = _len_class(L, flag_max, short_max, flat_max)
            a_shp[i] = _shape(high[start:i + 1], low[start:i + 1], atr[i]); a_pole[i] = pole
            state[i] = "ARMED" if L >= min_len else "WATCH"

    return pd.DataFrame({
        "date": d["date"].to_numpy(),
        "in_consol": in_c, "state": state,
        "top": a_top, "low": a_low, "depth_pct": np.round(a_dep * 100, 1),
        "len_days": a_len, "len_class": a_lc, "shape": a_shp,
        "pole_pct": np.round(a_pole * 100, 1),
        "pivot_entry": a_top, "cheat_entry": a_low,
        "breakout": brk,
    })


def consol_episodes(h):
    """One row per consolidation episode (contiguous in_consol run). Reports the state at end."""
    out = []; cur = None
    for r in h.itertuples():
        if r.in_consol:
            if cur is None:
                cur = dict(start=r.date, end=r.date, max_len=r.len_days, max_depth=r.depth_pct,
                           top=r.top, low=r.low, shape=r.shape, len_class=r.len_class,
                           pole_pct=r.pole_pct, broke=r.breakout)
            else:
                cur["end"] = r.date
                cur["max_len"] = max(cur["max_len"], r.len_days)
                if np.isfinite(r.depth_pct): cur["max_depth"] = np.nanmax([cur["max_depth"], r.depth_pct])
                cur["shape"] = r.shape; cur["len_class"] = r.len_class; cur["low"] = r.low
                cur["pole_pct"] = r.pole_pct
                cur["broke"] = cur["broke"] or r.breakout
        else:
            if cur: out.append(cur); cur = None
    if cur: out.append(cur)
    return pd.DataFrame(out)
