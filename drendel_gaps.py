"""
Drendel Gaps — stateful detector + point-in-time HISTORY (Nick Drendel methodology).

Gap UP   forms at bar i when  low[i]  > close[i-1].  Zone = [close[i-1]=floor(FIXED), low[i]=top].
Gap DOWN forms at bar i when  high[i] < close[i-1].  Zone = [high[i]=bottom, close[i-1]=top(FIXED)].

Per-gap state machine, walked forward:
  GAP UP:   close<floor -> FILLED | low<=top -> tested 'support'   | close<top -> top ratchets DOWN to max(close,floor)
  GAP DOWN: close>top   -> FILLED | high>=bottom -> tested 'resistance' | close>bottom -> bottom ratchets UP to min(close,top)

Each gap is TAGGED at formation with 4 non-exclusive type flags:
  base       tight prior consolidation (range of prior base_len bars <= base_k * ATR)
  shakeout   gap-UP right after a sharp flush (close fell >= shake_drop% over prior shake_m bars)
  wedge_pop  gap-UP through the wedge_ema EMA from below, with a higher low vs recent swing  (-> Kell)
  earnings   big gap on heavy volume: |gap| >= earn_gap% AND vol >= earn_volmult * avg(vol, earn_volavg)  (-> EP/BGU)

run_drendel(df) returns (active_gaps_at_last_bar, history_df, formed, filled).
history_df has ONE ROW PER BAR with features AS OF THAT BAR (point-in-time, backtest-safe).
"""
import pandas as pd

TYPES = ["base", "shakeout", "wedge_pop", "earnings"]


def run_drendel(df,
                base_len=10, base_k=2.5,
                shake_m=5, shake_drop=8.0,
                wedge_ema=20, wedge_swing=10,
                earn_gap=4.0, earn_volmult=1.5, earn_volavg=50):
    high_s = df["high"]; low_s = df["low"]; close_s = df["close"]
    has_vol = "volume" in df.columns
    vol = df["volume"].values if has_vol else None
    h = high_s.values; l = low_s.values; c = close_s.values
    n = len(df)

    # ---- type helper series (all prior-bar aligned, point-in-time) ----
    prev_c = close_s.shift(1)
    tr = pd.concat([(high_s - low_s), (high_s - prev_c).abs(), (low_s - prev_c).abs()], axis=1).max(axis=1)
    atr_pr   = tr.rolling(base_len, min_periods=base_len).mean().shift(1).values          # ATR as of prior bar
    baserng  = (high_s.rolling(base_len).max() - low_s.rolling(base_len).min()).shift(1).values  # prior base_len-bar range
    retm     = (close_s.shift(1) / close_s.shift(1 + shake_m) - 1).values                 # return into the gap over shake_m bars
    ema      = close_s.ewm(span=wedge_ema, adjust=False).mean()
    ema_pr   = ema.shift(1).values                                                        # EMA prior bar
    ema_now  = ema.values
    pc_arr   = prev_c.values
    swinglo  = low_s.rolling(wedge_swing).min().shift(1).values                           # prior swing low
    avgvol   = (pd.Series(vol).rolling(earn_volavg).mean().shift(1).values if has_vol else None)  # avg vol prior earn_volavg bars

    def nn(x):
        return x == x  # True if not NaN

    def gap_types(kind, i, gsz):
        base = shake = wedge = earn = False
        if nn(baserng[i]) and nn(atr_pr[i]) and atr_pr[i] > 0:
            base = bool(baserng[i] <= base_k * atr_pr[i])
        if kind == "up" and nn(retm[i]):
            shake = bool(retm[i] <= -shake_drop / 100.0)
        if kind == "up":
            was_below = nn(ema_pr[i]) and pc_arr[i] < ema_pr[i]
            now_above = nn(ema_now[i]) and c[i] > ema_now[i]
            higher_lo = nn(swinglo[i]) and l[i] > swinglo[i]
            wedge = bool(was_below and now_above and higher_lo)
        if gsz is not None and abs(gsz) >= earn_gap and has_vol and avgvol is not None:
            if nn(avgvol[i]) and avgvol[i] > 0:
                earn = bool(vol[i] >= earn_volmult * avgvol[i])
        return {"base": base, "shakeout": shake, "wedge_pop": wedge, "earnings": earn}

    active, rows = [], []
    last_up = last_down = -1
    formed = filled = 0

    for i in range(n):
        gu = gd = False; gsz = None
        if i > 0:
            keep = []
            for g in active:                          # update existing gaps with bar i
                if g["kind"] == "up":
                    if c[i] < g["bottom"]: filled += 1; continue
                    if l[i] <= g["top"]:  g["state"] = "support"
                    if c[i] < g["top"]:   g["top"] = max(c[i], g["bottom"])
                else:
                    if c[i] > g["top"]:   filled += 1; continue
                    if h[i] >= g["bottom"]: g["state"] = "resistance"
                    if c[i] > g["bottom"]:  g["bottom"] = min(c[i], g["top"])
                keep.append(g)
            active = keep
            pc = c[i - 1]                              # form new gap from (i-1, i)
            if l[i] > pc:
                gsz = round((l[i] - pc) / pc * 100, 2) if pc > 0 else None
                g = {"kind": "up", "bottom": pc, "top": l[i], "state": "raw", "formed": i}
                g.update(gap_types("up", i, gsz))
                active.append(g); last_up = i; gu = True; formed += 1
            elif h[i] < pc:
                gsz = round((pc - h[i]) / pc * 100, 2) if pc > 0 else None
                g = {"kind": "down", "bottom": h[i], "top": pc, "state": "raw", "formed": i}
                g.update(gap_types("down", i, gsz))
                active.append(g); last_down = i; gd = True; formed += 1

        price = c[i]                                   # snapshot features AS OF bar i
        ups = [g for g in active if g["kind"] == "up"]
        dns = [g for g in active if g["kind"] == "down"]
        below = [g for g in ups if g["top"] <= price]
        above = [g for g in dns if g["bottom"] >= price]
        nsup = max(below, key=lambda g: g["top"]) if below else None
        nres = min(above, key=lambda g: g["bottom"]) if above else None
        lo, hi = price * 0.75, price * 1.25            # +/-25% near band
        near_ups = [g for g in ups if g["top"] >= lo and g["bottom"] <= hi]
        near_dns = [g for g in dns if g["top"] >= lo and g["bottom"] <= hi]

        def flags(g, pre):
            return {f"{pre}_{t}": (bool(g[t]) if g else None) for t in TYPES}

        def counts(gaps, pre):
            return {f"{pre}_{t}_n": int(sum(1 for g in gaps if g[t])) for t in TYPES}

        row = {
            "date": df["date"].iloc[i] if "date" in df.columns else i,
            "close": round(price, 2),
            "gap_up_today": gu, "gap_down_today": gd, "new_gap_size_pct": gsz,
            "support_gaps": len(ups), "resistance_gaps": len(dns), "net_gap_bias": len(ups) - len(dns),
            "support_gaps_near": len(near_ups), "resistance_gaps_near": len(near_dns),
            "net_gap_bias_near": len(near_ups) - len(near_dns),
            "in_support_gap": any(g["bottom"] <= price <= g["top"] for g in ups),
            "in_resistance_gap": any(g["bottom"] <= price <= g["top"] for g in dns),
            "nearest_support_top": round(nsup["top"], 2) if nsup else None,
            "nearest_support_bottom": round(nsup["bottom"], 2) if nsup else None,
            "pct_to_support": round((nsup["top"] / price - 1) * 100, 2) if nsup else None,
            "nearest_resistance_bottom": round(nres["bottom"], 2) if nres else None,
            "nearest_resistance_top": round(nres["top"], 2) if nres else None,
            "pct_to_resistance": round((nres["bottom"] / price - 1) * 100, 2) if nres else None,
            "days_since_gap_up": (i - last_up) if last_up >= 0 else None,
            "days_since_gap_down": (i - last_down) if last_down >= 0 else None,
        }
        row.update(flags(nsup, "nearest_support"))
        row.update(flags(nres, "nearest_resistance"))
        row.update(counts(near_ups, "near_support"))
        row.update(counts(near_dns, "near_resistance"))
        rows.append(row)

    return active, pd.DataFrame(rows), formed, filled


def active_zone_list(active):
    """Full list of active zones (for the latest-bar setups_daily detail), with type flags."""
    out = []
    for g in sorted(active, key=lambda x: x["top"], reverse=True):
        z = {"kind": g["kind"], "state": g["state"],
             "bottom": round(g["bottom"], 2), "top": round(g["top"], 2), "formed_i": g["formed"]}
        for t in TYPES:
            z[t] = bool(g[t])
        out.append(z)
    return out


# ---------- weekly support ----------
def resample_weekly(df):
    """Daily OHLC(+volume) -> weekly (W-FRI)."""
    d = df.copy()
    d["dt"] = pd.to_datetime(d["date"])
    d = d.set_index("dt")
    agg = {"open": ("open", "first"), "high": ("high", "max"),
           "low": ("low", "min"), "close": ("close", "last")}
    if "volume" in d.columns:
        agg["volume"] = ("volume", "sum")
    w = d.resample("W-FRI").agg(**agg).dropna()
    w["date"] = w.index.date
    return w.reset_index(drop=True)


def compute_both(df):
    """Returns {'D': (active, hist), 'W': (active, hist)} for one ticker."""
    dA, dH, _, _ = run_drendel(df)
    wA, wH, _, _ = run_drendel(resample_weekly(df))
    return {"D": (dA, dH), "W": (wA, wH)}
