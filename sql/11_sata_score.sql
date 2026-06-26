-- 11_sata_score.sql
-- Materializes the sata_score TABLE via the per-band-table pattern.
-- WHY: the live v_sata_score view is a 10-way join of JS-UDF-heavy band views;
-- materializing it directly (CREATE TABLE AS SELECT * FROM v_sata_score) runs the
-- single-threaded JS UDFs across all tickers in one giant query and exceeds the
-- nightly timeout. Instead we build each band to its own table, then assemble
-- sata_score from the 10 tables (pure join, no JS, seconds).
--
-- band_04 uses the VALIDATED optimized form: it reads `hist` from t_band_03
-- instead of recomputing MACD via sata_macd (proven 0 diffs vs band_04_elder).
--
-- Order matters: t_band_03 must build BEFORE t_band_04 (band_04 reads its hist).

-- ===== 5 FAST native-SQL band tables =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_01` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_01_overhead`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_05` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_05_mansfield`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0607` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_0607_price_vs_ma`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0809` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_0809_ma_slope`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_10` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_10_breakout`;

-- ===== 3 SLOW JS-UDF band tables =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_02` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_02_volume`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_03` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_03_macd`;

-- band_04 optimized: ema13 via sata_ema, hist JOINed from t_band_03 (no sata_macd)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_04` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker,
         ARRAY_AGG(close    ORDER BY wk) AS closes,
         ARRAY_AGG(wk       ORDER BY wk) AS wks,
         ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  GROUP BY ticker
),
ema AS (
  SELECT ticker, wks[OFFSET(i)] AS wk, idxs[OFFSET(i)] AS idx, e[OFFSET(i)] AS ema13
  FROM (SELECT ticker, wks, idxs,
               `stonks-498420.stonks_data.sata_ema`(closes, 13.0) AS e
        FROM series),
       UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i
),
joined AS (
  SELECT e.ticker, e.wk, e.idx, e.ema13, m.hist
  FROM ema e
  LEFT JOIN `stonks-498420.stonks_data.t_band_03` m USING (ticker, wk)
)
SELECT ticker, wk, ema13, hist,
  CASE WHEN idx < 35 THEN NULL
       ELSE ((CASE WHEN ema13 > LAG(ema13) OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)
           + (CASE WHEN hist  > LAG(hist)  OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)) - 1
  END AS row4_elder
FROM joined;

-- ===== assemble sata_score from the 10 band tables =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score` CLUSTER BY ticker AS
SELECT
  b1.ticker, b1.wk,
  ( IFNULL(CASE WHEN b1.row1_overhead     =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b2.row2_volume       =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b3.row3_macd_gt_signal=1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b4.row4_elder        =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b5.row5_mansfield    =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b67.row6_close_gt_40w=1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b67.row7_close_gt_10w=1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b89.row8_30w_rising  =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b89.row9_10w_rising  =1 THEN 1 ELSE 0 END,0)
  + IFNULL(CASE WHEN b10.row10_breakout   =1 THEN 1 ELSE 0 END,0)
  ) AS sata_score,
  b1.row1_overhead, b2.row2_volume, b3.row3_macd_gt_signal, b4.row4_elder,
  b5.row5_mansfield, b67.row6_close_gt_40w, b67.row7_close_gt_10w,
  b89.row8_30w_rising, b89.row9_10w_rising, b10.row10_breakout
FROM `stonks-498420.stonks_data.t_band_01`   b1
LEFT JOIN `stonks-498420.stonks_data.t_band_02`   b2  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_03`   b3  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_04`   b4  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_05`   b5  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0607` b67 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0809` b89 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_10`   b10 USING (ticker, wk);
