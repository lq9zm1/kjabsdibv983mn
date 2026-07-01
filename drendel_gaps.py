"""
Drendel Gaps — stateful detector + point-in-time HISTORY (Nick Drendel methodology).

Gap UP   forms at bar i when  low[i]  > close[i-1].  Zone = [close[i-1]=floor(FIXED), low[i]=top].
Gap DOWN forms at bar i when  high[i] < close[i-1].  Zone = [high[i]=bottom, close[i-1]=top(FIXED)].

Per-gap state machine, walked forward:
  GAP UP:   close<floor -> FILLED | low<=top -> tested 'support'   | close<top -> top ratchets DOWN to max(close,floor)
  GAP DOWN: close>top   -> FILLED | high>=bottom -> tested 'resistance' | close>bottom -> bottom ratchets UP to min(close,top)

run_drendel(df) returns (active_gaps_at_last_bar, history_df).
history_df has ONE ROW PER BAR with the gap features AS OF THAT BAR (point-in-time, safe for backtesting).
"""
import pandas as pd


def run_drendel(df):
    h = df["high"].values; l = df["low"].values; c = df["close"].values
    n = len(df)
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
                gsz = round((l[i] - pc) / pc * 100, 2)
                active.append({"kind": "up", "bottom": pc, "top": l[i], "state": "raw", "formed": i})
                last_up = i; gu = True; formed += 1
            elif h[i] < pc:
                gsz = round((pc - h[i]) / pc * 100, 2)
                active.append({"kind": "down", "bottom": h[i], "top": pc, "state": "raw", "formed": i})
                last_down = i; gd = True; formed += 1

        price = c[i]                                   # snapshot features AS OF bar i
        ups = [g for g in active if g["kind"] == "up"]
        dns = [g for g in active if g["kind"] == "down"]
        below = [g for g in ups if g["top"] <= price]
        above = [g for g in dns if g["bottom"] >= price]
        nsup = max(below, key=lambda g: g["top"]) if below else None
        nres = min(above, key=lambda g: g["bottom"]) if above else None
        near_pct = 25.0
        lo, hi = price * (1 - near_pct / 100), price * (1 + near_pct / 100)
        sup_near = sum(1 for g in ups if g["top"] >= lo and g["bottom"] <= hi)
        res_near = sum(1 for g in dns if g["top"] >= lo and g["bottom"] <= hi)
        rows.append({
            "date": df["date"].iloc[i] if "date" in df.columns else i,
            "close": round(price, 2),
            "gap_up_today": gu, "gap_down_today": gd, "new_gap_size_pct": gsz,
            "support_gaps": len(ups), "resistance_gaps": len(dns), "net_gap_bias": len(ups) - len(dns),
            "support_gaps_near": sup_near, "resistance_gaps_near": res_near, "net_gap_bias_near": sup_near - res_near,
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
        })
    return active, pd.DataFrame(rows), formed, filled


def active_zone_list(active):
    """Full list of active zones (for the latest-bar setups_daily detail)."""
    out = []
    for g in sorted(active, key=lambda x: x["top"], reverse=True):
        out.append({"kind": g["kind"], "state": g["state"],
                    "bottom": round(g["bottom"], 2), "top": round(g["top"], 2), "formed_i": g["formed"]})
    return out


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads/NASDAQ_HOOD__1D.csv"
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["time"], unit="s").dt.date
    df = df.sort_values("time").reset_index(drop=True)

    active, hist, formed, filled = run_drendel(df)
    print(f"bars={len(df)}  {df['date'].iloc[0]} -> {df['date'].iloc[-1]}   formed={formed} filled={filled} active={len(active)}\n")

    cols = ["date","close","gap_up_today","gap_down_today","new_gap_size_pct","support_gaps",
            "resistance_gaps","in_support_gap","in_resistance_gap","nearest_support_top",
            "pct_to_support","nearest_resistance_bottom","pct_to_resistance","days_since_gap_down"]

    print("=== HISTORY sample — around the disputed 140 gap (2025-11-04 .. 11-12) ===")
    win = hist[(hist["date"]>=pd.to_datetime("2025-11-04").date()) & (hist["date"]<=pd.to_datetime("2025-11-12").date())]
    print(win[cols].to_string(index=False))

    print("\n=== HISTORY tail (last 5 bars) ===")
    print(hist[cols].tail(5).to_string(index=False))

    print(f"\n=== LATEST snapshot row (this is the setups_daily row for HOOD) ===")
    print(hist.iloc[-1].to_string())
    print(f"\nhistory shape: {hist.shape}  (one row per bar, point-in-time, backtest-ready)")


# ---------- weekly support ----------
def resample_weekly(df):
    """Daily OHLC -> weekly (W-FRI). df must have date, open, high, low, close."""
    d = df.copy()
    d["dt"] = pd.to_datetime(d["date"])
    d = d.set_index("dt")
    w = d.resample("W-FRI").agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last")
    ).dropna()
    w["date"] = w.index.date
    return w.reset_index(drop=True)


def compute_both(df):
    """Returns {'D': (active, hist), 'W': (active, hist)} for one ticker."""
    dA, dH, _, _ = run_drendel(df)
    wdf = resample_weekly(df)
    wA, wH, _, _ = run_drendel(wdf)
    return {"D": (dA, dH), "W": (wA, wH)}
