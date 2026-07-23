-- ============================================================================
-- 14b_sata_hist_nightly — keep the long-term SATA history (sata_score_daily_hist) CURRENT.
-- Goes in sql/ (top-level, so run_rebuilds globs it); runs AFTER 14_sata_daily.
--
-- WHY: 14_sata_daily builds v_sata_score_daily (a view over the last 750 days) and materializes only
-- the last 90 days into sata_score_daily. NOTHING feeds sata_score_daily_hist — it had no builder in
-- the repo, so it froze (was stale ~4 weeks). This step upserts the recent window from
-- v_sata_score_daily into sata_score_daily_hist so it never goes stale again.
--
-- SELF-HEALING: the source window starts 7 days before the history table's current MAX(date), and
-- v_sata_score_daily exposes every day up to today — so the FIRST run catches up the entire gap
-- (June 26 -> today) in one shot, and every later night just tops up the last few days. The deep
-- 2017->2023 history already in the table is NEVER touched (it's outside the 750-day view anyway).
--
-- ⚠️ Column list below assumes sata_score_daily_hist has EXACTLY the v_sata_score_daily columns:
--    ticker, date, sata_score_d, row1_overhead, row2_volume, row3_macd_gt_signal, row4_elder,
--    row5_mansfield, row6_close_gt_40w, row7_close_gt_10w, row8_30w_rising, row9_10w_rising, row10_breakout
--    Verify with:  SELECT * FROM `stonks-498420.stonks_data.sata_score_daily_hist` ORDER BY date DESC LIMIT 3;
--    If the table has extra/different columns, adjust the INSERT/UPDATE lists to match.
-- ============================================================================

-- Recompute the recent-window floor ONCE (a table-subquery can't live in a MERGE ON predicate).
DECLARE hist_floor DATE DEFAULT (
  SELECT DATE_SUB(MAX(date), INTERVAL 7 DAY) FROM `stonks-498420.stonks_data.sata_score_daily_hist`
);

MERGE `stonks-498420.stonks_data.sata_score_daily_hist` T
USING (
  SELECT ticker, date, sata_score_d,
    row1_overhead, row2_volume, row3_macd_gt_signal, row4_elder, row5_mansfield,
    row6_close_gt_40w, row7_close_gt_10w, row8_30w_rising, row9_10w_rising, row10_breakout
  FROM `stonks-498420.stonks_data.v_sata_score_daily`
  WHERE date >= hist_floor
) S
ON  T.ticker = S.ticker AND T.date = S.date
    AND T.date >= hist_floor
WHEN MATCHED THEN UPDATE SET
  sata_score_d       = S.sata_score_d,
  row1_overhead      = S.row1_overhead,
  row2_volume        = S.row2_volume,
  row3_macd_gt_signal= S.row3_macd_gt_signal,
  row4_elder         = S.row4_elder,
  row5_mansfield     = S.row5_mansfield,
  row6_close_gt_40w  = S.row6_close_gt_40w,
  row7_close_gt_10w  = S.row7_close_gt_10w,
  row8_30w_rising    = S.row8_30w_rising,
  row9_10w_rising    = S.row9_10w_rising,
  row10_breakout     = S.row10_breakout
WHEN NOT MATCHED THEN INSERT
  (ticker, date, sata_score_d, row1_overhead, row2_volume, row3_macd_gt_signal, row4_elder,
   row5_mansfield, row6_close_gt_40w, row7_close_gt_10w, row8_30w_rising, row9_10w_rising, row10_breakout)
  VALUES
  (S.ticker, S.date, S.sata_score_d, S.row1_overhead, S.row2_volume, S.row3_macd_gt_signal, S.row4_elder,
   S.row5_mansfield, S.row6_close_gt_40w, S.row7_close_gt_10w, S.row8_30w_rising, S.row9_10w_rising, S.row10_breakout);

-- verify after run:
-- SELECT MIN(date) mn, MAX(date) mx, COUNT(*) n FROM `stonks-498420.stonks_data.sata_score_daily_hist`;
