"""
13-Week Breakout -> 21-EMA Pullback detector (Stonks Track B setup).

Two-phase state machine, point-in-time (backtest-safe), computed on daily bars:
  PHASE 1 breakout: close makes a new N-day high (daily, N=65 ~= 13wk) AND/OR a
                    weekly close makes a new 13-week high -> arms a WATCH window.
  PHASE 2 entry:    while in the watch window, a daily bar kisses the 21-EMA and
                    holds (low<=EMA<=close, first touch) -> pullback_entry alert.
Multiple entries per breakout are allowed. days_since_bo_(daily/weekly) is recorded
on every bar so avg breakout->entry lag is computable in backtest.

run_breakout(df) -> (history_df one-row-per-bar, n_breakouts, n_entries).
df needs: date, open, high, low, close  (volume optional, unused here).
"""
import pandas as pd


def run_breakout(df, hi_len=65, wk_hi_len=13, ema_len=21, watch_n=25):
    df = df.sort_values("date").reset_index(drop=True)
    close = df["close"]; low = df["low"]; high = df["high"]
    c = close.values; l = low.values
    n = len(df)

    # ---- daily 65-day CLOSING-high breakout ----
    prior_hi = close.rolling(hi_len).max().shift(1).values      # highest close, prior hi_len bars
    ema = close.ewm(span=ema_len, adjust=False).mean()
    e = ema.values

    # ---- weekly 13-week breakout, mapped to the last trading day of each week ----
    wk = pd.to_datetime(df["date"]).dt.to_period("W-FRI")
    wk_close = close.groupby(wk).last()
    wk_bo = (wk_close > wk_close.rolling(wk_hi_len).max().shift(1))
    last_idx_per_wk = df.groupby(wk).apply(lambda g: g.index[-1])
    bo_weekly = pd.Series(False, index=df.index)
    for period, is_bo in wk_bo.items():
        if bool(is_bo):
            bo_weekly.iloc[last_idx_per_wk[period]] = True
    bo_weekly = bo_weekly.values

    rows = []
    last_bo_d = last_bo_w = -1
    n_bo = n_entry = 0

    for i in range(n):
        # breakout flags this bar
        bd = (prior_hi[i] == prior_hi[i]) and c[i] > prior_hi[i]   # daily closing 65d high
        bw = bool(bo_weekly[i])                                    # weekly 13wk high (week-end)
        if bd:
            last_bo_d = i
        if bw:
            last_bo_w = i
        if bd or bw:
            n_bo += 1

        dsd = (i - last_bo_d) if last_bo_d >= 0 else None
        dsw = (i - last_bo_w) if last_bo_w >= 0 else None
        watch_d = dsd is not None and 0 <= dsd <= watch_n
        watch_w = dsw is not None and 0 <= dsw <= watch_n
        in_watch = watch_d or watch_w

        # pullback entry: first-touch kiss & hold of the 21-EMA, while in watch
        touch = (e[i] == e[i]) and l[i] <= e[i] <= c[i]
        prior_above = i > 0 and (e[i - 1] == e[i - 1]) and l[i - 1] > e[i - 1]
        entry = bool(in_watch and touch and prior_above)
        if entry:
            n_entry += 1

        rows.append({
            "date": df["date"].iloc[i],
            "close": round(c[i], 2),
            "ema_21": round(e[i], 2) if e[i] == e[i] else None,
            "pct_to_21ema": round((c[i] / e[i] - 1) * 100, 2) if (e[i] == e[i] and e[i] > 0) else None,
            "bo_daily": bool(bd), "bo_weekly": bw,
            "days_since_bo_daily": dsd, "days_since_bo_weekly": dsw,
            "in_watch_daily": bool(watch_d), "in_watch_weekly": bool(watch_w),
            "in_watch": bool(in_watch),
            "pullback_entry": entry,
        })

    return pd.DataFrame(rows), n_bo, n_entry


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads/NASDAQ_HOOD__1D.csv"
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["time"], unit="s").dt.date
    H, nbo, nent = run_breakout(df)
    print(f"bars={len(df)}  breakout-bars={nbo}  pullback-entries={nent}")
    ent = H[H["pullback_entry"]]
    print("\nPULLBACK ENTRIES:")
    print(ent[["date","close","ema_21","pct_to_21ema","days_since_bo_daily","days_since_bo_weekly"]].to_string(index=False))
