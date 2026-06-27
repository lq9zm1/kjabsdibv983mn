-- ============================================================================
--  SATA Score — DAILY layer (Tier-3)   |   nightly file, runs after 11/12/13
--  Project: stonks-498420   Dataset: stonks_data
--
--  Mirrors sata_rebuild.sql on DAILY bars. Reuses the deployed UDFs
--  sata_ema / sata_macd / sata_volume UNCHANGED (timeframe-agnostic).
--
--  Self-contained & idempotent (all CREATE OR REPLACE). Dependency order:
--    base daily view -> 8 band views -> v_sata_score_daily -> sata_score_daily TABLE.
--  The views are definitions only; the single CREATE TABLE at the bottom is the
--  one compute step (runs the JS UDFs over the recent window) and is what the
--  dashboard joins. Nightly glob runs sql/*.sql in filename order: 11->12->13->14.
--
--  Design notes:
--   * Windowed to the last 1000 calendar days for warmup; EMAs converge so recent
--     dates are bit-exact vs full history. NEVER score early-window (warmup) dates.
--   * Daily Mansfield lookback = 52 (calibrated vs sas Mansfield RS (D): 10/10 sign
--     on calibration names; 99.6% band-sign match on the 6/12 clean universe).
--   * All params identical to weekly (SMA10/30/40, EMA13, MACD 12/26/9, breakout 13,
--     Ichimoku 9/26/52 offset 26, volume 13/4/3/13).
--
--  Validated 2026-06-12 vs stageanalysis.net sas SATA (D):
--    weekly-clean US (n=1680): 96.3% exact / 99.5% within +/-1
--    all US (n=1813):          95.0% exact / 99.1% within +/-1
--  Residual = the same per-ticker dividend-adjustment vendor divergence as the
--  weekly score (HIGH bias) = the daily OHLCV ceiling, not a logic gap.
--
--  LIVE DEPENDENCY: the latest-day daily SATA is only as fresh as spx_daily
--  (a one-time TVC:SPX load). Step 3 (spx_daily nightly refresh, EODHD GSPC.INDX)
--  makes the latest bar live; until then the latest day lags to the last SPX load.
-- ============================================================================


-- ========================= 1. BASE DAILY VIEW ===============================
-- Dividend-adjusted daily bars, windowed to recent ~1000 days. EOD guard applied.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_daily` AS
WITH d AS (
  SELECT ticker, date, SAFE_DIVIDE(adj_close, close) AS f,
         open, high, low, adj_close, volume
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1000 DAY) AND date < CURRENT_DATE()
),
adj AS (
  SELECT ticker, date, open*f AS open, high*f AS high, low*f AS low,
         adj_close AS close, volume
  FROM d WHERE f IS NOT NULL
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS day_index
FROM adj;


-- ========================= 2. BAND VIEWS ====================================

-- Row 1 — Overhead Resistance (Ichimoku cloud, offset 26)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_01_overhead_daily` AS
WITH base AS (
  SELECT ticker, date, close,
    MAX(high) OVER w9 AS hh9, MIN(low) OVER w9 AS ll9, COUNT(*) OVER w9 AS n9,
    MAX(high) OVER w26 AS hh26, MIN(low) OVER w26 AS ll26, COUNT(*) OVER w26 AS n26,
    MAX(high) OVER w52 AS hh52, MIN(low) OVER w52 AS ll52, COUNT(*) OVER w52 AS n52
  FROM `stonks-498420.stonks_data.v_sata_daily`
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
  FROM `stonks-498420.stonks_data.v_sata_daily`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
)
SELECT ticker, date,
  CASE WHEN n40<40 THEN NULL WHEN close>sma40 THEN 1 ELSE -1 END AS row6_close_gt_40w,
  CASE WHEN n10<10 THEN NULL WHEN close>sma10 THEN 1 ELSE -1 END AS row7_close_gt_10w
FROM b;


-- Rows 8 & 9 — SMA30 / SMA10 slope (rising/falling/flat)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0809_ma_slope_daily` AS
WITH b AS (
  SELECT ticker, date, day_index,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w30 AS sma30
  FROM `stonks-498420.stonks_data.v_sata_daily`
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
  FROM `stonks-498420.stonks_data.v_sata_daily`
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
  FROM `stonks-498420.stonks_data.v_sata_daily` eq
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


-- Row 3 — MACD line vs signal (warmup day_index < 34)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_03_macd_daily` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY date) AS closes,
         ARRAY_AGG(date ORDER BY date) AS dates, ARRAY_AGG(day_index ORDER BY date) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_daily` GROUP BY ticker
),
calc AS (SELECT ticker, dates, idxs, `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS arr FROM series)
SELECT ticker, dates[OFFSET(i)] AS date,
  CASE WHEN idxs[OFFSET(i)]<34 THEN NULL WHEN arr[OFFSET(i)].macd>arr[OFFSET(i)].signal THEN 1 ELSE -1 END AS row3_macd_gt_signal
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(dates)-1)) AS i;


-- Row 4 — Elder Impulse (EMA13 slope + MACD-hist slope; warmup day_index < 35)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_04_elder_daily` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY date) AS closes,
         ARRAY_AGG(date ORDER BY date) AS dates, ARRAY_AGG(day_index ORDER BY date) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_daily` GROUP BY ticker
),
calc AS (
  SELECT ticker, dates, idxs,
    `stonks-498420.stonks_data.sata_ema`(closes,13) AS ema13,
    `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS marr
  FROM series
),
r AS (
  SELECT ticker, dates[OFFSET(i)] AS date, idxs[OFFSET(i)] AS idx,
         ema13[OFFSET(i)] AS ema13, marr[OFFSET(i)].hist AS hist
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(dates)-1)) AS i
)
SELECT ticker, date,
  CASE WHEN idx<35 THEN NULL
       ELSE ((CASE WHEN ema13>LAG(ema13) OVER (PARTITION BY ticker ORDER BY date) THEN 1 ELSE 0 END)
           + (CASE WHEN hist >LAG(hist)  OVER (PARTITION BY ticker ORDER BY date) THEN 1 ELSE 0 END)) - 1
  END AS row4_elder
FROM r;


-- Row 2 — Volume (Force-Index money-flow crossover; 29-bar IPO warmup)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_02_volume_daily` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY date) AS closes,
         ARRAY_AGG(CAST(volume AS FLOAT64) ORDER BY date) AS vols, ARRAY_AGG(date ORDER BY date) AS dates
  FROM `stonks-498420.stonks_data.v_sata_daily` GROUP BY ticker
),
calc AS (SELECT ticker, dates, `stonks-498420.stonks_data.sata_volume`(closes,vols,13,4,3,13) AS arr FROM series)
SELECT ticker, dates[OFFSET(i)] AS date,
  CASE WHEN i < 29 THEN 0 ELSE arr[OFFSET(i)].p2 END AS row2_volume
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(dates) - 1)) AS i;


-- ========================= 3. DAILY SATA SCORE VIEW =========================
-- sata_score_d = count of GREEN (p==1) bands across all 10.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_score_daily` AS
SELECT w.ticker, w.date,
    (CASE WHEN b01.row1_overhead=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b02.row2_volume=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b03.row3_macd_gt_signal=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b04.row4_elder=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b05.row5_mansfield=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row6_close_gt_40w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row7_close_gt_10w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row8_30w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row9_10w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b10.row10_breakout=1 THEN 1 ELSE 0 END) AS sata_score_d,
  b01.row1_overhead, b02.row2_volume, b03.row3_macd_gt_signal, b04.row4_elder,
  b05.row5_mansfield, b67.row6_close_gt_40w, b67.row7_close_gt_10w,
  b89.row8_30w_rising, b89.row9_10w_rising, b10.row10_breakout
FROM `stonks-498420.stonks_data.v_sata_daily` w
LEFT JOIN `stonks-498420.stonks_data.band_01_overhead_daily`      b01 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_02_volume_daily`        b02 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_03_macd_daily`          b03 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_04_elder_daily`         b04 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield_daily`     b05 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_0607_price_vs_ma_daily` b67 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_0809_ma_slope_daily`    b89 USING (ticker, date)
LEFT JOIN `stonks-498420.stonks_data.band_10_breakout_daily`      b10 USING (ticker, date);


-- ========================= 4. MATERIALIZE (the one compute step) ============
-- Snapshot the recent valid window to a clustered table the dashboard joins.
-- 90-day retention is plenty for latest-day SATA (D) + Score Chg (D).
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score_daily` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.v_sata_score_daily`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

-- ============================================================================
--  END. After running: SELECT ticker, date, sata_score_d
--                      FROM stonks_data.sata_score_daily
--                      WHERE date = (SELECT MAX(date) FROM stonks_data.sata_score_daily)
--                      LIMIT 10;
-- ============================================================================
