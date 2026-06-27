-- 13_weekly_detail.sql
-- Weekly per-ticker fields for dashboard parity with the sas export. One row per ticker-week.
-- Includes: weekly OHLCV, 10/30/40W MAs (+ %to + above flags), Donchian channels
--   (HH/LL/HC/LC at 52/26/13/4W), weekly dollar-volume family, single-source Mansfield.
-- Runs nightly after sata_score (11 -> 12 -> 13).

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.weekly_detail` CLUSTER BY ticker AS
WITH w AS (
  SELECT ticker, wk, close, high, low, volume,
    close * volume AS dvol,
    AVG(close) OVER w10 AS sma10, COUNT(close) OVER w10 AS n10,
    AVG(close) OVER w30 AS sma30, COUNT(close) OVER w30 AS n30,
    AVG(close) OVER w40 AS sma40, COUNT(close) OVER w40 AS n40,
    AVG(close * volume) OVER w10 AS advol10,
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
)
SELECT
  w.ticker, w.wk,
  w.close  AS close_w,
  w.high   AS high_w,
  w.low    AS low_w,
  w.volume AS volume_w,
  CASE WHEN w.n10>=10 THEN w.sma10 END AS sma_10w,
  CASE WHEN w.n30>=30 THEN w.sma30 END AS sma_30w,
  CASE WHEN w.n40>=40 THEN w.sma40 END AS sma_40w,
  CASE WHEN w.n10>=10 THEN ROUND((w.close/NULLIF(w.sma10,0)-1)*100,2) END AS pct_to_10w,
  CASE WHEN w.n30>=30 THEN ROUND((w.close/NULLIF(w.sma30,0)-1)*100,2) END AS pct_to_30w,
  CASE WHEN w.n40>=40 THEN ROUND((w.close/NULLIF(w.sma40,0)-1)*100,2) END AS pct_to_40w,
  CASE WHEN w.n10>=10 THEN CAST(w.close > w.sma10 AS INT64) END AS abv_10w,
  CASE WHEN w.n30>=30 THEN CAST(w.close > w.sma30 AS INT64) END AS abv_30w,
  CASE WHEN w.n40>=40 THEN CAST(w.close > w.sma40 AS INT64) END AS abv_40w,
  ROUND(w.hh52,2) AS channel_hh_52w, ROUND(w.ll52,2) AS channel_ll_52w, ROUND(w.hc52,2) AS channel_hc_52w, ROUND(w.lc52,2) AS channel_lc_52w,
  ROUND(w.hh26,2) AS channel_hh_26w, ROUND(w.ll26,2) AS channel_ll_26w, ROUND(w.hc26,2) AS channel_hc_26w, ROUND(w.lc26,2) AS channel_lc_26w,
  ROUND(w.hh13,2) AS channel_hh_13w, ROUND(w.ll13,2) AS channel_ll_13w, ROUND(w.hc13,2) AS channel_hc_13w, ROUND(w.lc13,2) AS channel_lc_13w,
  ROUND(w.hh04,2) AS channel_hh_4w,  ROUND(w.ll04,2) AS channel_ll_4w,  ROUND(w.hc04,2) AS channel_hc_4w,  ROUND(w.lc04,2) AS channel_lc_4w,
  ROUND(w.dvol, 0) AS dollar_vol_w,
  CASE WHEN w.n10>=10 THEN ROUND(w.advol10, 0) END AS avg_dollar_vol_10w,
  CASE WHEN w.n10>=10 THEN ROUND(SAFE_DIVIDE(w.dvol, w.advol10), 2) END AS rel_vol_w,
  m5.mansfield_line AS mansfield_w
FROM w
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield` m5 USING (ticker, wk);
