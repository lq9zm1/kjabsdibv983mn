-- 11_sata_score.sql
-- Materializes the weekly sata_score TABLE.
--
-- OPTIMIZED 2026-07-22 (bit-exact; validate with the 0-diff query before committing):
--   The weekly base was being re-resampled from price_history (back to 2000) ~9x per run —
--   once per band view + once in the assembly = the 4.2 GB scan. Now it's resampled ONCE into
--   sata_weekly_bars, and every band + the assembly read that small table. Same UDFs, same
--   logic, same inputs -> identical output. The shared live band views (band_01_overhead, ...)
--   and v_sata_score are LEFT UNTOUCHED (dashboard unaffected); this file just builds its own
--   materialized base + t_band tables + sata_score.
--
--   band_04 still reuses t_band_03.hist (no MACD recompute). Order: t_band_03 before t_band_04.

-- ===== 0. weekly base, resampled ONCE (was ~9 re-resamples) =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_weekly_bars` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.v_sata_weekly`;


-- ===== 5 FAST native-SQL band tables (now read sata_weekly_bars) =====

-- t_band_01 — Overhead Resistance (Ichimoku cloud, offset 26)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_01` CLUSTER BY ticker AS
WITH base AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w9 AS hh9, MIN(low) OVER w9 AS ll9, COUNT(*) OVER w9 AS n9,
    MAX(high) OVER w26 AS hh26, MIN(low) OVER w26 AS ll26, COUNT(*) OVER w26 AS n26,
    MAX(high) OVER w52 AS hh52, MIN(low) OVER w52 AS ll52, COUNT(*) OVER w52 AS n52
  FROM `stonks-498420.stonks_data.sata_weekly_bars`
  WINDOW w9 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 8 PRECEDING AND CURRENT ROW),
         w26 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
         w52 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
),
ich AS (
  SELECT ticker, wk, close,
    CASE WHEN n26>=26 THEN (hh26+ll26)/2 END AS kij,
    CASE WHEN n9>=9 AND n26>=26 THEN ((hh9+ll9)/2 + (hh26+ll26)/2)/2 END AS spanA_now,
    CASE WHEN n52>=52 THEN (hh52+ll52)/2 END AS spanB_now
  FROM base
),
d AS (
  SELECT ticker, wk, close, kij,
    LAG(spanA_now,26) OVER (PARTITION BY ticker ORDER BY wk) AS spanA,
    LAG(spanB_now,26) OVER (PARTITION BY ticker ORDER BY wk) AS spanB
  FROM ich
)
SELECT ticker, wk, close, kij, spanA, spanB,
  CASE
    WHEN spanA IS NULL OR spanB IS NULL THEN 0
    WHEN close>GREATEST(spanA,spanB) AND close>kij THEN 1
    WHEN ((spanB>spanA) AND NOT(close>GREATEST(spanA,spanB)) AND close<kij)
      OR (NOT(spanB>spanA) AND close<LEAST(spanA,spanB) AND close<kij) THEN -1
    ELSE 0
  END AS row1_overhead
FROM d;

-- t_band_0607 — close vs SMA40 / SMA10
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0607` CLUSTER BY ticker AS
WITH b AS (
  SELECT ticker, wk, close,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w40 AS sma40,
    COUNT(close) OVER w10 AS n10, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.sata_weekly_bars`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, close, sma10, sma40,
  CASE WHEN n40<40 THEN NULL WHEN close>sma40 THEN 1 ELSE -1 END AS row6_close_gt_40w,
  CASE WHEN n10<10 THEN NULL WHEN close>sma10 THEN 1 ELSE -1 END AS row7_close_gt_10w
FROM b;

-- t_band_0809 — SMA30 / SMA10 slope
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0809` CLUSTER BY ticker AS
WITH b AS (
  SELECT ticker, wk, wk_index,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w30 AS sma30
  FROM `stonks-498420.stonks_data.sata_weekly_bars`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
),
s AS (
  SELECT ticker, wk, wk_index, sma10, sma30,
    LAG(sma10) OVER (PARTITION BY ticker ORDER BY wk) AS sma10_prev,
    LAG(sma30) OVER (PARTITION BY ticker ORDER BY wk) AS sma30_prev
  FROM b
)
SELECT ticker, wk,
  CASE WHEN wk_index<31 THEN NULL WHEN sma30>sma30_prev THEN 1 WHEN sma30<sma30_prev THEN -1 ELSE 0 END AS row8_30w_rising,
  CASE WHEN wk_index<11 THEN NULL WHEN sma10>sma10_prev THEN 1 WHEN sma10<sma10_prev THEN -1 ELSE 0 END AS row9_10w_rising
FROM s;

-- t_band_10 — Breakout (Donchian 13W, event flash)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_10` CLUSTER BY ticker AS
WITH b AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w AS channel_hh_13w, MIN(low) OVER w AS channel_ll_13w, COUNT(*) OVER w AS n
  FROM `stonks-498420.stonks_data.sata_weekly_bars`
  WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
)
SELECT ticker, wk, close, channel_hh_13w, channel_ll_13w,
  CASE WHEN n<13 THEN 0 WHEN close>channel_hh_13w THEN 1 WHEN close<channel_ll_13w THEN -1 ELSE 0 END AS row10_breakout
FROM b;

-- t_band_05 — Mansfield RS (close/SPX vs 52W avg, SAME-WEEK cash SPX, no lag)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_05` CLUSTER BY ticker AS
WITH j AS (
  SELECT eq.ticker, eq.wk,
    SAFE_DIVIDE(eq.close, spx.spx_close)*100 AS ratio
  FROM `stonks-498420.stonks_data.sata_weekly_bars` eq
  LEFT JOIN `stonks-498420.stonks_data.v_spx_weekly` spx USING (wk)
),
m AS (
  SELECT ticker, wk, ratio,
    AVG(ratio) OVER w AS maRS, COUNT(ratio) OVER w AS n
  FROM j WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, ratio, maRS, ((ratio/maRS)-1)*100 AS mansfield_line,
  CASE WHEN n<52 THEN NULL WHEN ratio>maRS THEN 1 ELSE -1 END AS row5_mansfield
FROM m;


-- ===== 3 JS-UDF band tables (now read sata_weekly_bars) =====

-- t_band_02 — Volume (Force-Index money-flow crossover; 29-bar IPO warmup)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_02` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(CAST(volume AS FLOAT64) ORDER BY wk) AS vols, ARRAY_AGG(wk ORDER BY wk) AS wks
  FROM `stonks-498420.stonks_data.sata_weekly_bars` GROUP BY ticker
),
calc AS (SELECT ticker, wks, `stonks-498420.stonks_data.sata_volume`(closes,vols,13,4,3,13) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].fast AS vol_fast, arr[OFFSET(i)].slow AS vol_slow,
  CASE WHEN i < 29 THEN 0 ELSE arr[OFFSET(i)].p2 END AS row2_volume
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks) - 1)) AS i;

-- t_band_03 — MACD line vs signal (warmup wk_index < 34)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_03` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(wk ORDER BY wk) AS wks, ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.sata_weekly_bars` GROUP BY ticker
),
calc AS (SELECT ticker, wks, idxs, `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].macd AS macd, arr[OFFSET(i)].signal AS signal, arr[OFFSET(i)].hist AS hist,
  CASE WHEN idxs[OFFSET(i)]<34 THEN NULL WHEN arr[OFFSET(i)].macd>arr[OFFSET(i)].signal THEN 1 ELSE -1 END AS row3_macd_gt_signal
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i;

-- t_band_04 — Elder (ema13 via sata_ema; hist JOINed from t_band_03, no sata_macd recompute)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_04` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker,
         ARRAY_AGG(close    ORDER BY wk) AS closes,
         ARRAY_AGG(wk       ORDER BY wk) AS wks,
         ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.sata_weekly_bars`
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


-- ===== assemble sata_score from the 10 band tables (spine now sata_weekly_bars, not a resample) =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score` CLUSTER BY ticker AS
SELECT
  w.ticker, w.wk,
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
FROM `stonks-498420.stonks_data.sata_weekly_bars` w
LEFT JOIN `stonks-498420.stonks_data.t_band_01`   b1  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_02`   b2  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_03`   b3  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_04`   b4  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_05`   b5  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0607` b67 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0809` b89 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_10`   b10 USING (ticker, wk);
