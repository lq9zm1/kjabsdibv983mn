-- 13_weekly_detail.sql
-- Weekly per-ticker fields for dashboard parity with the sas export. One row per ticker-week.
-- Percent columns stored as DECIMALS (Sheets formats as %). % to MA = founder convention (MA/close-1).
-- RVOL = share-based Volume(W)/AvgVol(10W). Mansfield single-source from band_05. Reads stage_engine
-- for S2A/S4A anchors. Runs nightly after sata_score (11 -> 12 -> 13).

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.weekly_detail` CLUSTER BY ticker AS
WITH w AS (
  SELECT ticker, wk, close, high, low, volume,
    close * volume AS dvol,
    AVG(close) OVER w10 AS sma10, COUNT(close) OVER w10 AS n10,
    AVG(close) OVER w30 AS sma30, COUNT(close) OVER w30 AS n30,
    AVG(close) OVER w40 AS sma40, COUNT(close) OVER w40 AS n40,
    AVG(close * volume) OVER w10 AS advol10,
    AVG(CAST(volume AS FLOAT64)) OVER w10 AS avgvol10,
    MAX(high) OVER c52 AS hh52, MIN(low) OVER c52 AS ll52, MAX(close) OVER c52 AS hc52, MIN(close) OVER c52 AS lc52,
    MAX(high) OVER c26 AS hh26, MIN(low) OVER c26 AS ll26, MAX(close) OVER c26 AS hc26, MIN(close) OVER c26 AS lc26,
    MAX(high) OVER c13 AS hh13, MIN(low) OVER c13 AS ll13, MAX(close) OVER c13 AS hc13, MIN(close) OVER c13 AS lc13,
    MAX(high) OVER c04 AS hh04, MIN(low) OVER c04 AS ll04, MAX(close) OVER c04 AS hc04, MIN(close) OVER c04 AS lc04
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW
    w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN  9 PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW),
    c52 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW),
    c26 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
    c13 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 12 PRECEDING AND CURRENT ROW),
    c04 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN  3 PRECEDING AND CURRENT ROW)
),
joined AS (
  SELECT w.*, se.broad_stage, se.wks_since
  FROM w
  LEFT JOIN `stonks-498420.stonks_data.stage_engine_v2` se USING (ticker, wk)
),
lvl AS (
  SELECT *,
    LAST_VALUE(IF(broad_stage='S2' AND wks_since=1, close, NULL) IGNORE NULLS)
      OVER (PARTITION BY ticker ORDER BY wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_s2_px,
    LAST_VALUE(IF(broad_stage='S4' AND wks_since=1, close, NULL) IGNORE NULLS)
      OVER (PARTITION BY ticker ORDER BY wk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_s4_px
  FROM joined
)
SELECT
  lvl.ticker, lvl.wk,
  lvl.close  AS close_w,
  lvl.high   AS high_w,
  lvl.low    AS low_w,
  lvl.volume AS volume_w,
  CASE WHEN lvl.n10>=10 THEN lvl.sma10 END AS sma_10w,
  CASE WHEN lvl.n30>=30 THEN lvl.sma30 END AS sma_30w,
  CASE WHEN lvl.n40>=40 THEN lvl.sma40 END AS sma_40w,
  CASE WHEN lvl.n10>=10 THEN ROUND(lvl.sma10/NULLIF(lvl.close,0)-1, 4) END AS pct_to_10w,
  CASE WHEN lvl.n30>=30 THEN ROUND(lvl.sma30/NULLIF(lvl.close,0)-1, 4) END AS pct_to_30w,
  CASE WHEN lvl.n40>=40 THEN ROUND(lvl.sma40/NULLIF(lvl.close,0)-1, 4) END AS pct_to_40w,
  CASE WHEN lvl.n10>=10 THEN CAST(lvl.close > lvl.sma10 AS INT64) END AS abv_10w,
  CASE WHEN lvl.n30>=30 THEN CAST(lvl.close > lvl.sma30 AS INT64) END AS abv_30w,
  CASE WHEN lvl.n40>=40 THEN CAST(lvl.close > lvl.sma40 AS INT64) END AS abv_40w,
  ROUND(lvl.hh52,2) AS channel_hh_52w, ROUND(lvl.ll52,2) AS channel_ll_52w, ROUND(lvl.hc52,2) AS channel_hc_52w, ROUND(lvl.lc52,2) AS channel_lc_52w,
  ROUND(lvl.hh26,2) AS channel_hh_26w, ROUND(lvl.ll26,2) AS channel_ll_26w, ROUND(lvl.hc26,2) AS channel_hc_26w, ROUND(lvl.lc26,2) AS channel_lc_26w,
  ROUND(lvl.hh13,2) AS channel_hh_13w, ROUND(lvl.ll13,2) AS channel_ll_13w, ROUND(lvl.hc13,2) AS channel_hc_13w, ROUND(lvl.lc13,2) AS channel_lc_13w,
  ROUND(lvl.hh04,2) AS channel_hh_4w,  ROUND(lvl.ll04,2) AS channel_ll_4w,  ROUND(lvl.hc04,2) AS channel_hc_4w,  ROUND(lvl.lc04,2) AS channel_lc_4w,
  ROUND(lvl.dvol, 0) AS dollar_vol_w,
  CASE WHEN lvl.n10>=10 THEN ROUND(lvl.advol10, 0) END AS avg_dollar_vol_10w,
  CASE WHEN lvl.n10>=10 THEN ROUND(lvl.avgvol10, 0) END AS avg_vol_10w,
  CASE WHEN lvl.n10>=10 THEN ROUND(SAFE_DIVIDE(CAST(lvl.volume AS FLOAT64), lvl.avgvol10), 4) END AS rel_vol_w,
  ROUND(lvl.last_s2_px, 2) AS last_s2_price,
  CASE WHEN lvl.last_s2_px IS NOT NULL THEN ROUND(lvl.close/lvl.last_s2_px - 1, 4) END AS pct_since_s2,
  ROUND(lvl.last_s4_px, 2) AS last_s4_price,
  CASE WHEN lvl.last_s4_px IS NOT NULL THEN ROUND(lvl.close/lvl.last_s4_px - 1, 4) END AS pct_since_s4,
  m5.mansfield_line AS mansfield_w
FROM lvl
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield` m5 USING (ticker, wk);
