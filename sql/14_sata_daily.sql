-- ============================================================================
--  SATA Score — DAILY layer (Tier-3)   |   nightly file, runs after 11/12/13
--  Project: stonks-498420   Dataset: stonks_data
--
--  OPTIMIZED 2026-07-22 (growth-proof; all changes bit-exact or already-validated):
--   1. band_04 no longer re-runs sata_macd — it reads `hist` from the combined MACD pass
--      (same optimization proven 0-diff in the weekly, 11_sata_score t_band_04).
--   2. bands 02/03/04 share ONE ARRAY_AGG of the daily bars (was 3 separate scans+aggs);
--      each JS UDF (sata_volume / sata_macd / sata_ema) now runs exactly once per ticker.
--   3. Warmup window 750 -> 450 calendar days. Output is still the last 90 days, whose
--      earliest bar retains ~248 trading-day warmup -> EMA26 residual ~5e-9 => BIT-EXACT
--      vs the 750-day build (the vendor-divergence ceiling below is untouched). Output is
--      a fixed 90-day rolling window, so this file's cost is now FLAT as history grows
--      (scales only with ticker count).
--
--  Reuses the deployed UDFs sata_ema / sata_macd / sata_volume UNCHANGED.
--  Self-contained & idempotent. Dependency order:
--    sata_daily_bars TABLE -> band views -> v_sata_score_daily -> sata_score_daily TABLE.
--
--  Daily Mansfield lookback = 52. Validated 2026-06-12 vs sas SATA (D): 96.3% exact /
--  99.5% +/-1 (residual = dividend-vendor divergence; unaffected by the warmup change).
--
--  WARMUP WINDOW = 450 days (was 750). 90d output + ~360d warmup. Bit-exact vs 750
--  (EMA26 residual ~5e-9 at the earliest kept bar). Bump this literal if you ever want more.
-- ============================================================================


-- ========================= 1. BASE DAILY BARS (MATERIALIZED) =================
-- One price_history scan (MONTH-partition pruned to the 450-day warmup window).
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_daily_bars` CLUSTER BY ticker AS
WITH d AS (
  SELECT ticker, date, SAFE_DIVIDE(adj_close, close) AS f,
         open, high, low, adj_close, volume
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 450 DAY) AND date < CURRENT_DATE()
),
adj AS (
  SELECT ticker, date, open*f AS open, high*f AS high, low*f AS low,
         adj_close AS close, volume
  FROM d WHERE f IS NOT NULL
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS day_index
FROM adj;


-- ========================= 2. BAND VIEWS =====================================
-- Fast native-SQL bands (cheap window functions) — unchanged.

-- Row 1 — Overhead Resistance (Ichimoku cloud, offset 26)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_01_overhead_daily` AS
WITH base AS (
  SELECT ticker, date, close,
    MAX(high) OVER w9 AS hh9, MIN(low) OVER w9 AS ll9, COUNT(*) OVER w9 AS n9,
    MAX(high) OVER w26 AS hh26, MIN(low) OVER w26 AS ll26, COUNT(*) OVER w26 AS n26,
    MAX(high) OVER w52 AS hh52, MIN(low) OVER w52 AS ll52, COUNT(*) OVER w52 AS n52
  FROM `stonks-498420.stonks_data.sata_daily_bars`
  WINDOW w9 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW),
         w26 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
         w52 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
),
ich AS (
  SELECT ticker, date, close,
    CASE WHEN n26>=26 THEN (hh26+ll26)/2 END AS kij,
    CASE WHEN n9>=9 AND n26>=26 THEN ((hh9+ll9)/2 + (hh26+ll26)/2)/2 END AS spanA_now,
    CASE WHEN n52>=52 THEN (hh52+ll52)/2 END AS spanB_now
  FROM base
),
dd AS (
  SELECT ticker, date, close, kij,
    LAG(spanA_now,26) OVER (PARTITION BY ticker ORDER BY date) AS spanA,
    LAG(spanB_now,26) OVER (PARTITION BY ticker ORDER BY date) AS spanB
  FROM ich
)
SELECT ticker, date,
  CASE
    WHEN spanA IS NULL OR spanB IS NULL THEN 0
    WHEN close>GREATEST(spanA,spanB) AND close>kij THEN 1
    WHEN ((spanB>spanA) AND NOT(close>GREATEST(spanA,spanB)) AND close<kij)
      OR (NOT(spanB>spanA) AND close<LEAST(spanA,spanB) AND close<kij) THEN -1
    ELSE 0
  END AS row1_overhead
FROM dd;


-- Rows 6 & 7 — close vs SMA40 / SMA10
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0607_price_vs_ma_daily` AS
WITH b AS (
  SELECT ticker, date, close,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w40 AS sma40,
    COUNT(close) OVER w10 AS n10, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.sata_daily_bars`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
)
SELECT ticker, date,
  CASE WHEN n40<40 THEN NULL WHEN close>sma40 THEN 1 ELSE -1 END AS row6_close_gt_40w,
  CASE WHEN n10<10 THEN NULL WHEN close>sma10 THEN 1 ELSE -1 END AS row7_close_gt_10w
FROM b;


-- Rows 8 & 9 — SMA30 / SMA10 slope
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0809_ma_slope_daily` AS
WITH b AS (
  SELECT ticker, date, day_index,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w30 AS sma30
  FROM `stonks-498420.stonks_data.sata_daily_bars`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w30 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
),
s AS (
  SELECT ticker, date, day_index, sma10, sma30,
    LAG(sma10) OVER (PARTITION BY ticker ORDER BY date) AS sma10_prev,
    LAG(sma30) OVER (PARTITION BY ticker ORDER BY date) AS sma30_prev
  FROM b
)
SELECT ticker, date,
  CASE WHEN day_index<31 THEN NULL WHEN sma30>sma30_prev THEN 1 WHEN sma30<sma30_prev THEN -1 ELSE 0 END AS row8_30w_rising,
  CASE WHEN day_index<11 THEN NULL WHEN sma10>sma10_prev THEN 1 WHEN sma10<sma10_prev THEN -1 ELSE 0 END AS row9_10w_rising
FROM s;


-- Row 10 — Breakout (Donchian 13, event flash)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_10_breakout_daily` AS
WITH b AS (
  SELECT ticker, date, close,
    MAX(high) OVER w AS hh13, MIN(low) OVER w AS ll13, COUNT(*) OVER w AS n
  FROM `stonks-498420.stonks_data.sata_daily_bars`
  WINDOW w AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
)
SELECT ticker, date,
  CASE WHEN n<13 THEN 0 WHEN close>hh13 THEN 1 WHEN close<ll13 THEN -1 ELSE 0 END AS row10_breakout
FROM b;


-- Row 5 — Mansfield RS (close/SPX vs 52-day avg, same-day, NO lag)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_05_mansfield_daily` AS
WITH spx AS (
  SELECT DATE(TIMESTAMP_SECONDS(`time`)) AS date, close AS spx_close
  FROM `stonks-498420.stonks_data.spx_daily`
),
j AS (
  SELECT eq.ticker, eq.date, SAFE_DIVIDE(eq.close, spx.spx_close)*100 AS ratio
  FROM `stonks-498420.stonks_data.sata_daily_bars` eq
  LEFT JOIN spx USING (date)
),
m AS (
  SELECT ticker, date, ratio,
    AVG(ratio) OVER w AS maRS, COUNT(ratio) OVER w AS n
  FROM j WINDOW w AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
)
SELECT ticker, date,
  CASE WHEN n<52 THEN NULL WHEN ratio>maRS THEN 1 ELSE -1 END AS row5_mansfield
FROM m;


-- Rows 2,3,4 — COMBINED JS-UDF band (one ARRAY_AGG; sata_volume + sata_macd + sata_ema each once).
--   row3 = MACD vs signal.  row4 = EMA13-slope + MACD-hist-slope (hist reused from the SAME macd
--   pass, not recomputed).  row2 = Force-Index money-flow crossover.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_020304_daily` AS
WITH series AS (
  SELECT ticker,
    ARRAY_AGG(close ORDER BY date)                    AS closes,
    ARRAY_AGG(CAST(volume AS FLOAT64) ORDER BY date)  AS vols,
    ARRAY_AGG(date ORDER BY date)                     AS dates,
    ARRAY_AGG(day_index ORDER BY date)                AS idxs
  FROM `stonks-498420.stonks_data.sata_daily_bars`
  GROUP BY ticker
),
calc AS (
  SELECT ticker, dates, idxs,
    `stonks-498420.stonks_data.sata_macd`(closes, 12, 26, 9)          AS marr,
    `stonks-498420.stonks_data.sata_ema`(closes, 13)                  AS ema13arr,
    `stonks-498420.stonks_data.sata_volume`(closes, vols, 13, 4, 3, 13) AS varr
  FROM series
),
u AS (
  SELECT ticker,
    dates[OFFSET(i)] AS date,
    i                AS pos,
    idxs[OFFSET(i)]  AS idx,
    marr[OFFSET(i)].macd   AS macd,
    marr[OFFSET(i)].signal AS signal,
    marr[OFFSET(i)].hist   AS hist,
    ema13arr[OFFSET(i)]    AS ema13,
    varr[OFFSET(i)].p2     AS vol_p2
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(dates)-1)) AS i
)
SELECT ticker, date,
  CASE WHEN pos < 29 THEN 0 ELSE vol_p2 END                              AS row2_volume,
  CASE WHEN idx < 34 THEN NULL WHEN macd > signal THEN 1 ELSE -1 END     AS row3_macd_gt_signal,
  CASE WHEN idx < 35 THEN NULL
       ELSE ((CASE WHEN ema13 > LAG(ema13) OVER (PARTITION BY ticker ORDER BY date) THEN 1 ELSE 0 END)
           + (CASE WHEN hist  > LAG(hist)  OVER (PARTITION BY ticker ORDER BY date) THEN 1 ELSE 0 END)) - 1
  END                                                                    AS row4_elder
FROM u;


-- ========================= 3. DAILY SATA SCORE VIEW =========================
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_score_daily` AS
SELECT w.ticker, w.date,
    (CASE WHEN b01.row1_overhead=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b234.row2_volume=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b234.row3_macd_gt_signal=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b234.row4_elder=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b05.row5_mansfield=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row6_close_gt_40w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row7_close_gt_10w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row8_30w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row9_10w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b10.row10_breakout=1 THEN 1 ELSE 0 END) AS sata_score_d,
  b01.row1_overhead, b234.row2_volume, b234.row3_macd_gt_signal, b234.row4_elder,
  b05.row5_mansfield, b67.row6_close_gt_40w, b67.row7_close_gt_10w,
  b89.row8_30w_rising, b89.row9_10w_rising, b10.row10_breakout
FROM `stonks-498420.stonks_data.sata_daily_bars` w
LEFT JOIN `stonks-498420.stonks_data.band_01_overhead_daily`      b01  USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_020304_daily`           b234 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield_daily`     b05  USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_0607_price_vs_ma_daily` b67  USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_0809_ma_slope_daily`    b89  USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_10_breakout_daily`      b10  USING (ticker, date);


-- ========================= 4. MATERIALIZE LATEST SCORES =====================
-- 90-day retention: latest-day SATA (D) + Score Chg (D) + 6/12 re-checks.
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score_daily` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.v_sata_score_daily`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);


-- NOTE (No-Drop rule): the old band_02_volume_daily / band_03_macd_daily / band_04_elder_daily
-- views are now UNUSED (superseded by band_020304_daily) but are left in place, not dropped.
-- They still resolve against sata_daily_bars if anything references them.

-- ============================================================================
--  END.
-- ============================================================================
