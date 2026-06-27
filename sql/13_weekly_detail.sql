-- 13_weekly_detail.sql
-- Weekly per-ticker fields for dashboard parity with the sas export. One row per ticker-week.
-- Mansfield comes single-source from band_05_mansfield. Runs nightly after sata_score
-- (filename order: 11_sata_score -> 12_stage_engine -> 13_weekly_detail).

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.weekly_detail` CLUSTER BY ticker AS
WITH w AS (
  SELECT ticker, wk, close, high, low, volume,
    AVG(close)   OVER w10 AS sma10, COUNT(close) OVER w10 AS n10,
    AVG(close)   OVER w30 AS sma30, COUNT(close) OVER w30 AS n30,
    AVG(close)   OVER w40 AS sma40, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN  9 PRECEDING AND CURRENT ROW),
         w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
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
  m5.mansfield_line AS mansfield_w
FROM w
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield` m5 USING (ticker, wk);
