-- 11b_weekly_geometry.sql
-- Shared weekly MA geometry (SMA 10/30/40W + pct-to-MA). Computed IDENTICALLY to
-- 13_weekly_detail.sql (same source v_sata_weekly, same windows, same ROUND/NULLIF/gating)
-- so the stage engine and weekly_detail use ONE consistent geometry source (option B).
-- Filename sorts 11_ < 11b_ < 12_  -> runs after sata_score, before the stage engines.
-- Percent = founder convention: sma/close - 1  (NEGATIVE = price ABOVE the MA).

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.weekly_geometry` CLUSTER BY ticker AS
SELECT
  ticker, wk,
  close AS close_w,
  CASE WHEN n10>=10 THEN sma10 END AS sma_10w,
  CASE WHEN n30>=30 THEN sma30 END AS sma_30w,
  CASE WHEN n40>=40 THEN sma40 END AS sma_40w,
  CASE WHEN n10>=10 THEN ROUND(sma10/NULLIF(close,0)-1, 4) END AS pct_to_10w,
  CASE WHEN n30>=30 THEN ROUND(sma30/NULLIF(close,0)-1, 4) END AS pct_to_30w,
  CASE WHEN n40>=40 THEN ROUND(sma40/NULLIF(close,0)-1, 4) END AS pct_to_40w
FROM (
  SELECT ticker, wk, close,
    AVG(close) OVER w10 AS sma10, COUNT(close) OVER w10 AS n10,
    AVG(close) OVER w30 AS sma30, COUNT(close) OVER w30 AS n30,
    AVG(close) OVER w40 AS sma40, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW
    w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN  9 PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
);
