-- 13W breakout -> 21-EMA pullback ENTRIES, enriched as-of the entry (no look-ahead)
-- with stage/substage, SATA + Mansfield rising flags, and Drendel support-gap confluence.
-- Filtered to pullback_entry rows (sparse) so the as-of joins stay cheap.
-- This is the backtest asset: filter substage/stage to drop late-stage, and
-- AVG(days_since_bo_*) by stage to measure the breakout->entry lag.

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_13w_breakout_entries` AS
WITH entries AS (
  SELECT ticker, date AS entry_date, close, pct_to_21ema,
         days_since_bo_daily, days_since_bo_weekly, bo_daily, bo_weekly
  FROM `stonks-498420.stonks_data.breakout_history`
  WHERE pullback_entry
),
-- stage as-of the LAST COMPLETED week before the entry (no look-ahead)
stg AS (
  SELECT e.ticker, e.entry_date, se.wk AS stage_week,
         se.broad_stage, se.stage, se.wks_since, se.came_from
  FROM entries e
  LEFT JOIN `stonks-498420.stonks_data.stage_engine` se
    ON se.ticker = e.ticker
   AND DATE(se.wk) < DATE_TRUNC(DATE(e.entry_date), WEEK(MONDAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.ticker, e.entry_date ORDER BY se.wk DESC) = 1
),
-- SATA score + week-over-week rising flag
sat AS (
  SELECT ticker, wk, sata_score AS sata_w,
         sata_score > LAG(sata_score) OVER (PARTITION BY ticker ORDER BY wk) AS sata_rising
  FROM `stonks-498420.stonks_data.sata_score`
),
-- Mansfield RS + week-over-week rising flag
man AS (
  SELECT ticker, wk, mansfield_w,
         mansfield_w > LAG(mansfield_w) OVER (PARTITION BY ticker ORDER BY wk) AS mansfield_rising
  FROM `stonks-498420.stonks_data.weekly_detail`
)
SELECT
  e.ticker, DATE(e.entry_date) AS entry_date, e.close, e.pct_to_21ema,
  e.days_since_bo_daily, e.days_since_bo_weekly, e.bo_daily, e.bo_weekly,
  -- stage (tags for filtering, not hard-coded)
  CASE WHEN stg.broad_stage = 'N' THEN 'Neutral' ELSE stg.broad_stage END AS broad_stage,
  stg.stage AS substage, stg.wks_since, stg.came_from, stg.stage_week,
  -- SATA quality
  sat.sata_w, sat.sata_rising,
  man.mansfield_w, man.mansfield_rising,
  -- Drendel support-gap confluence at the entry date
  g.in_support_gap, g.support_gaps_near, g.nearest_support_top, g.pct_to_support,
  g.near_support_base_n, g.near_support_shakeout_n,
  g.near_support_wedge_pop_n, g.near_support_earnings_n
FROM entries e
LEFT JOIN stg USING (ticker, entry_date)
LEFT JOIN sat ON sat.ticker = e.ticker AND sat.wk = stg.stage_week
LEFT JOIN man ON man.ticker = e.ticker AND man.wk = stg.stage_week
LEFT JOIN `stonks-498420.stonks_data.gap_history` g
  ON g.ticker = e.ticker AND DATE(g.date) = DATE(e.entry_date) AND g.tf = 'D';
