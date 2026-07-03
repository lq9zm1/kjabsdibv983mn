-- v_level_reclaim_entries (v2) — Level Reclaim / UNR family (daily). Console VIEW (sql/views/).
-- Reclaim of a key MA after undercut (stop = undercut low). GRADE = gap-support-at-MA (Drendel confluence,
-- validated in BQ) + RS-rising + SATA band_score (the daily bands that actually drive it):
--   band_score = r5_mansfield + r2_volume + r3_macd + r9_10w_rising  -  r8_30w_rising   (joined on the EXACT
--   reclaim date). Split-tested: band_score=4 -> ~84% win; monotonic up the ladder; r8 (30w-rising) HURTS.
--   A+ = gap-support & RS & band_score=4  |  A = & band_score>=1  |  B = & band_score<=0/NULL  |  C = neither.
-- 6/20 intraday trigger = R4.1, not here.

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_level_reclaim_entries` AS
WITH lr AS (
  SELECT * EXCEPT(date), DATE(date) AS d
  FROM `stonks-498420.stonks_data.level_reclaim_history`
  WHERE is_reclaim = TRUE
),
gap_asof AS (
  SELECT p.d, p.ticker, g.nearest_support_top AS gap_support, g.in_support_gap,
    ROW_NUMBER() OVER (PARTITION BY p.ticker, p.d ORDER BY DATE(g.date) DESC) AS rn
  FROM lr p LEFT JOIN `stonks-498420.stonks_data.gap_history` g
    ON g.ticker = p.ticker AND g.tf = 'D' AND DATE(g.date) <= p.d
),
stage_asof AS (
  SELECT p.d, p.ticker, se.broad_stage, se.stage AS substage, se.wks_since, se.came_from,
    ROW_NUMBER() OVER (PARTITION BY p.ticker, p.d ORDER BY se.wk DESC) AS rn
  FROM lr p LEFT JOIN `stonks-498420.stonks_data.stage_engine` se
    ON se.ticker = p.ticker AND se.wk < DATE_TRUNC(p.d, WEEK(MONDAY))
),
band AS (   -- SATA band states ON THE EXACT reclaim date
  SELECT lr.ticker, lr.d, s.sata_score_d AS sata_d,
    ( IF(s.row5_mansfield=1,1,0) + IF(s.row2_volume=1,1,0) + IF(s.row3_macd_gt_signal=1,1,0)
      + IF(s.row9_10w_rising=1,1,0) - IF(s.row8_30w_rising=1,1,0) ) AS band_score,
    s.row5_mansfield, s.row2_volume, s.row3_macd_gt_signal, s.row9_10w_rising, s.row8_30w_rising
  FROM lr JOIN `stonks-498420.stonks_data.sata_score_daily` s
    ON s.ticker = lr.ticker AND s.date = lr.d
),
j AS (
  SELECT lr.*,
    ga.gap_support, ga.in_support_gap,
    (COALESCE(ga.in_support_gap, FALSE)
       OR (ga.gap_support IS NOT NULL AND ABS(lr.close - ga.gap_support) / NULLIF(lr.close,0) <= 0.03)
    ) AS reclaim_at_gap_support,
    st.broad_stage, st.substage, st.wks_since,
    bd.sata_d, bd.band_score,
    bd.row5_mansfield, bd.row2_volume, bd.row3_macd_gt_signal, bd.row9_10w_rising, bd.row8_30w_rising
  FROM lr
  LEFT JOIN gap_asof   ga ON ga.ticker = lr.ticker AND ga.d = lr.d AND ga.rn = 1
  LEFT JOIN stage_asof st ON st.ticker = lr.ticker AND st.d = lr.d AND st.rn = 1
  LEFT JOIN band       bd ON bd.ticker = lr.ticker AND bd.d = lr.d
)
SELECT
  ticker,
  d               AS entry_date,
  CASE WHEN reclaim_at_gap_support AND rs_rising AND band_score = 4  THEN 'A+'
       WHEN reclaim_at_gap_support AND rs_rising AND band_score >= 1 THEN 'A'
       WHEN reclaim_at_gap_support AND rs_rising                     THEN 'B'
       ELSE 'C' END AS grade,
  which_ma, is_200ma_reclaim,
  close           AS entry_price,
  stop, risk_pct, undercut_low, undercut_bars, ext_atr_50, rs_rising,
  reclaim_at_gap_support, gap_support, in_support_gap,       -- confluence
  band_score, sata_d,                                        -- the daily band lever
  row5_mansfield, row2_volume, row3_macd_gt_signal, row9_10w_rising, row8_30w_rising,
  broad_stage, substage, wks_since                           -- context
FROM j;
