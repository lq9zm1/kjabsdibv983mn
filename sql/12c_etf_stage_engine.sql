-- sql/12c_etf_stage_engine.sql — NIGHTLY bit-exact ETF/sector SATA stage (self-contained, idempotent).
-- Runs in run_nightly.py step 4 after 12b_stage_engine_v2 (stock stages), so ETF stages stay as
-- fresh as stock stages. Recreates its *_etf views (cheap) + materializes t_band_*_etf ->
-- sata_score_etf -> weekly_geometry_etf -> stage_engine_v2_etf. Reuses shared UDFs + v_spx_weekly.
-- ~145 ETFs = negligible add to the stock SATA nightly. IF the nightly ever runs tight: this file
-- can drop to WEEKLY (the substage only changes weekly) without losing correctness.
--
-- ============================================================================
-- etf_stage_engine_build.sql — BIT-EXACT SATA stage engine for ETFs / sectors, ISOLATED.
-- Runs the SAME production band -> sata_score -> weekly_geometry -> stage_engine pipeline, but on
-- ETF prices, writing to *_etf tables so it CANNOT touch the stock nightly. "ETF in 2A" then means
-- precisely what "stock in 2A" means (same UDFs, same thresholds, same stage_walk_v2 lineage clock).
--
-- REUSES (shared, already in prod — NOT recreated): sata_ema, sata_macd, sata_volume, stage_walk_v2,
--   and v_spx_weekly (Mansfield SPX benchmark). Only the base weekly view is new (etf_prices source).
-- Output: `stage_engine_v2_etf` (ticker, wk, broad_stage, came_from, wks_since, stage, ...). `stage`
--   holds 2A/2/2B/2- (and 4A/4/4B/4-, 1, 3, N) — identical labeling to stocks.
-- RUN ORDER: this whole file top-to-bottom (band views -> t_band tables -> sata_score_etf ->
--   weekly_geometry_etf -> stage_engine_v2_etf). Off-hours; one-time + rerun when ETF prices update.
-- History floor = 2000-01-01 (same as the stock v_sata_weekly), so ETF stages align with stock stages.
-- ============================================================================

-- ── 0) base weekly bars for ETFs (mirror of v_sata_weekly, sourced from etf_prices) ──
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_weekly_etf` AS
WITH d AS (
  SELECT ticker, DATE(date) AS date, SAFE_DIVIDE(adj_close, close) AS f,
         open, high, low, adj_close, volume
  FROM `stonks-498420.stonks_data.etf_prices`
  WHERE DATE(date) >= DATE '2000-01-01' AND DATE(date) < CURRENT_DATE()
),
adj AS (
  SELECT ticker, date, open*f AS aopen, high*f AS ahigh, low*f AS alow, adj_close AS aclose, volume
  FROM d WHERE f IS NOT NULL
),
weekly AS (
  SELECT ticker,
    DATE_TRUNC(date, WEEK(MONDAY))                          AS wk,
    ARRAY_AGG(aopen  ORDER BY date ASC  LIMIT 1)[OFFSET(0)] AS open,
    MAX(ahigh)                                              AS high,
    MIN(alow)                                               AS low,
    ARRAY_AGG(aclose ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS close,
    ARRAY_AGG(aclose ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS adj_close,
    SUM(volume)                                             AS volume,
    MAX(date)                                               AS last_trading_day,
    COUNT(*)                                                AS days_in_week
  FROM adj GROUP BY ticker, wk
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY wk) AS wk_index FROM weekly;


-- ══ 1) BAND VIEWS (etf) ══════════════════════════════════════════════════
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_01_overhead_etf` AS
WITH base AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w9 AS hh9, MIN(low) OVER w9 AS ll9, COUNT(*) OVER w9 AS n9,
    MAX(high) OVER w26 AS hh26, MIN(low) OVER w26 AS ll26, COUNT(*) OVER w26 AS n26,
    MAX(high) OVER w52 AS hh52, MIN(low) OVER w52 AS ll52, COUNT(*) OVER w52 AS n52
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
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


-- Rows 6 & 7 — close vs SMA40 / SMA10.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0607_price_vs_ma_etf` AS
WITH b AS (
  SELECT ticker, wk, close,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w40 AS sma40,
    COUNT(close) OVER w10 AS n10, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, close, sma10, sma40,
  CASE WHEN n40<40 THEN NULL WHEN close>sma40 THEN 1 ELSE -1 END AS row6_close_gt_40w,
  CASE WHEN n10<10 THEN NULL WHEN close>sma10 THEN 1 ELSE -1 END AS row7_close_gt_10w
FROM b;


-- Rows 8 & 9 — SMA30 / SMA10 slope (rising/falling/flat).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0809_ma_slope_etf` AS
WITH b AS (
  SELECT ticker, wk, wk_index,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w30 AS sma30
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
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


-- Row 10 — Breakouts (Donchian 13W, event flash).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_10_breakout_etf` AS
WITH b AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w AS channel_hh_13w, MIN(low) OVER w AS channel_ll_13w, COUNT(*) OVER w AS n
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
  WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
)
SELECT ticker, wk, close, channel_hh_13w, channel_ll_13w,
  CASE WHEN n<13 THEN 0 WHEN close>channel_hh_13w THEN 1 WHEN close<channel_ll_13w THEN -1 ELSE 0 END AS row10_breakout
FROM b;


-- Row 5 — Mansfield RS (close/SPX vs 52W avg).
-- v6d FIX: SAME-WEEK cash SPX, NO lag. (Old version lagged SPX 1 week via LAG()
--  on a SPCFD feed — that double-bug caused ~3% razor-thin off-by-1 flips.)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_05_mansfield_etf` AS
WITH j AS (
  SELECT eq.ticker, eq.wk,
    SAFE_DIVIDE(eq.close, spx.spx_close)*100 AS ratio
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf` eq
  LEFT JOIN `stonks-498420.stonks_data.v_spx_weekly` spx USING (wk)   -- SAME WEEK, no lag
),
m AS (
  SELECT ticker, wk, ratio,
    AVG(ratio) OVER w AS maRS, COUNT(ratio) OVER w AS n
  FROM j WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, ratio, maRS, ((ratio/maRS)-1)*100 AS mansfield_line,
  CASE WHEN n<52 THEN NULL WHEN ratio>maRS THEN 1 ELSE -1 END AS row5_mansfield
FROM m;


-- Row 3 — MACD line vs signal (warmup wk_index < 34).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_03_macd_etf` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(wk ORDER BY wk) AS wks, ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf` GROUP BY ticker
),
calc AS (SELECT ticker, wks, idxs, `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].macd AS macd, arr[OFFSET(i)].signal AS signal, arr[OFFSET(i)].hist AS hist,
  CASE WHEN idxs[OFFSET(i)]<34 THEN NULL WHEN arr[OFFSET(i)].macd>arr[OFFSET(i)].signal THEN 1 ELSE -1 END AS row3_macd_gt_signal
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i;


-- Row 4 — Elder Impulse (EMA13 slope + MACD-hist slope; warmup wk_index < 35).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_04_elder_etf` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(wk ORDER BY wk) AS wks, ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf` GROUP BY ticker
),
calc AS (
  SELECT ticker, wks, idxs,
    `stonks-498420.stonks_data.sata_ema`(closes,13) AS ema13,
    `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS marr
  FROM series
),
r AS (
  SELECT ticker, wks[OFFSET(i)] AS wk, idxs[OFFSET(i)] AS idx,
         ema13[OFFSET(i)] AS ema13, marr[OFFSET(i)].hist AS hist
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i
)
SELECT ticker, wk, ema13, hist,
  CASE WHEN idx<35 THEN NULL
       ELSE ((CASE WHEN ema13>LAG(ema13) OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)
           + (CASE WHEN hist >LAG(hist)  OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)) - 1
  END AS row4_elder
FROM r;


-- Row 2 — Volume (Force-Index money-flow crossover). 29-bar IPO warmup gate.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_02_volume_etf` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(CAST(volume AS FLOAT64) ORDER BY wk) AS vols, ARRAY_AGG(wk ORDER BY wk) AS wks
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf` GROUP BY ticker
),
calc AS (SELECT ticker, wks, `stonks-498420.stonks_data.sata_volume`(closes,vols,13,4,3,13) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].fast AS vol_fast, arr[OFFSET(i)].slow AS vol_slow,
  CASE WHEN i < 29 THEN 0 ELSE arr[OFFSET(i)].p2 END AS row2_volume   -- 29-bar IPO warmup
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks) - 1)) AS i;


-- ========================= 4. SATA SCORE ====================================
-- sata_score = count of GREEN (p==1) bands across all 10.

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_score_etf` AS
SELECT w.ticker, w.wk,
    (CASE WHEN b01.row1_overhead=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b02.row2_volume=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b03.row3_macd_gt_signal=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b04.row4_elder=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b05.row5_mansfield=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row6_close_gt_40w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row7_close_gt_10w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row8_30w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row9_10w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b10.row10_breakout=1 THEN 1 ELSE 0 END) AS sata_score,
  b01.row1_overhead, b02.row2_volume, b03.row3_macd_gt_signal, b04.row4_elder,
  b05.row5_mansfield, b67.row6_close_gt_40w, b67.row7_close_gt_10w,
  b89.row8_30w_rising, b89.row9_10w_rising, b10.row10_breakout
FROM `stonks-498420.stonks_data.v_sata_weekly_etf` w
LEFT JOIN `stonks-498420.stonks_data.band_01_overhead_etf`      b01 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_02_volume_etf`        b02 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_03_macd_etf`          b03 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_04_elder_etf`         b04 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield_etf`     b05 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_0607_price_vs_ma_etf` b67 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_0809_ma_slope_etf`    b89 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_10_breakout_etf`      b10 USING (ticker, wk);

-- ============================================================================
--  END. After running: SELECT * FROM stonks_data.v_sata_score LIMIT 10;
-- ============================================================================


-- ══ 2) BAND TABLES + sata_score_etf ═════════════════════════════════════
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
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_01_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_01_overhead_etf`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_05_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_05_mansfield_etf`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0607_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_0607_price_vs_ma_etf`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_0809_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_0809_ma_slope_etf`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_10_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_10_breakout_etf`;

-- ===== 3 SLOW JS-UDF band tables =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_02_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_02_volume_etf`;

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_03_etf` CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.band_03_macd_etf`;

-- band_04 optimized: ema13 via sata_ema, hist JOINed from t_band_03 (no sata_macd)
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.t_band_04_etf` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker,
         ARRAY_AGG(close    ORDER BY wk) AS closes,
         ARRAY_AGG(wk       ORDER BY wk) AS wks,
         ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
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
  LEFT JOIN `stonks-498420.stonks_data.t_band_03_etf` m USING (ticker, wk)
)
SELECT ticker, wk, ema13, hist,
  CASE WHEN idx < 35 THEN NULL
       ELSE ((CASE WHEN ema13 > LAG(ema13) OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)
           + (CASE WHEN hist  > LAG(hist)  OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)) - 1
  END AS row4_elder
FROM joined;

-- ===== assemble sata_score from the 10 band tables =====
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score_etf` CLUSTER BY ticker AS
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
FROM `stonks-498420.stonks_data.t_band_01_etf`   b1
LEFT JOIN `stonks-498420.stonks_data.t_band_02_etf`   b2  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_03_etf`   b3  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_04_etf`   b4  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_05_etf`   b5  USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0607_etf` b67 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_0809_etf` b89 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.t_band_10_etf`   b10 USING (ticker, wk);


-- ══ 3) weekly_geometry_etf ══════════════════════════════════════════════
-- 11b_weekly_geometry.sql
-- Shared weekly MA geometry (SMA 10/30/40W + pct-to-MA). Computed IDENTICALLY to
-- 13_weekly_detail.sql (same source v_sata_weekly, same windows, same ROUND/NULLIF/gating)
-- so the stage engine and weekly_detail use ONE consistent geometry source (option B).
-- Filename sorts 11_ < 11b_ < 12_  -> runs after sata_score, before the stage engines.
-- Percent = founder convention: sma/close - 1  (NEGATIVE = price ABOVE the MA).

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.weekly_geometry_etf` CLUSTER BY ticker AS
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
  FROM `stonks-498420.stonks_data.v_sata_weekly_etf`
  WINDOW
    w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN  9 PRECEDING AND CURRENT ROW),
    w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
    w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
);


-- ══ 4) stage_engine_v2_etf ══════════════════════════════════════════════
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.stage_engine_v2_etf` CLUSTER BY ticker AS
WITH base AS (
  SELECT
    s.ticker, s.wk,
    CAST(s.sata_score AS FLOAT64) AS score,
    s.row7_close_gt_10w           AS row7,
    CASE WHEN s.row1_overhead      IS NOT NULL AND s.row2_volume        IS NOT NULL
          AND s.row3_macd_gt_signal IS NOT NULL AND s.row4_elder         IS NOT NULL
          AND s.row5_mansfield      IS NOT NULL AND s.row6_close_gt_40w  IS NOT NULL
          AND s.row7_close_gt_10w   IS NOT NULL AND s.row8_30w_rising    IS NOT NULL
          AND s.row9_10w_rising     IS NOT NULL AND s.row10_breakout     IS NOT NULL
         THEN 1.0 ELSE 0.0 END     AS valid,
    COALESCE(g.pct_to_30w, 0.0)    AS pct30,
    COALESCE(g.pct_to_40w, 0.0)    AS pct40
  FROM `stonks-498420.stonks_data.sata_score_etf` s
  LEFT JOIN `stonks-498420.stonks_data.weekly_geometry_etf` g USING (ticker, wk)
),
series AS (
  SELECT ticker,
    ARRAY_AGG(score ORDER BY wk) AS scores,
    ARRAY_AGG(valid ORDER BY wk) AS valids,
    ARRAY_AGG(pct30 ORDER BY wk) AS pct30s,
    ARRAY_AGG(pct40 ORDER BY wk) AS pct40s,
    ARRAY_AGG(row7  ORDER BY wk) AS r7s,
    ARRAY_AGG(wk    ORDER BY wk) AS wks
  FROM base
  GROUP BY ticker
),
calc AS (
  SELECT ticker, wks, r7s,
    `stonks-498420.stonks_data.stage_walk_v2`(scores, valids, pct30s, pct40s) AS w
  FROM series
),
flat AS (
  SELECT ticker,
    wks[OFFSET(i)]         AS wk,
    w[OFFSET(i)].broad     AS broad_stage,
    w[OFFSET(i)].came_from AS came_from,
    w[OFFSET(i)].wks_since AS wks_since,
    r7s[OFFSET(i)]         AS row7
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks) - 1)) AS i
),
labeled AS (
  SELECT ticker, wk, broad_stage, came_from, wks_since,
    CASE
      WHEN broad_stage = 'S2' THEN CONCAT('2',
           CASE WHEN wks_since = 1 THEN 'A' WHEN wks_since >= 41 THEN 'B' ELSE '' END,
           CASE WHEN row7 = -1 THEN '-' ELSE '' END)
      WHEN broad_stage = 'S4' THEN CONCAT('4',
           CASE WHEN wks_since = 1 THEN 'A' WHEN wks_since >= 41 THEN 'B' ELSE '' END,
           CASE WHEN row7 =  1 THEN '-' ELSE '' END)
      WHEN broad_stage = 'N' AND came_from = 'S2' THEN '3'
      WHEN broad_stage = 'N' AND came_from = 'S4' THEN '1'
      ELSE 'N'
    END AS stage
  FROM flat
)
SELECT ticker, wk, broad_stage, came_from, wks_since, stage,
  broad_stage <> LAG(broad_stage) OVER (PARTITION BY ticker ORDER BY wk) AS stage_changed,
  stage       <> LAG(stage)       OVER (PARTITION BY ticker ORDER BY wk) AS substage_changed
FROM labeled;


-- ══ VERIFY (run after the build) ═════════════════════════════════════════
-- Coverage + substage distribution for ETFs (should span 2000→present; stages 2A/2/2B/2-/1/3/4/N):
-- SELECT COUNT(*) n_rows, COUNT(DISTINCT ticker) n_etfs, MIN(wk) lo, MAX(wk) hi
-- FROM `stonks-498420.stonks_data.stage_engine_v2_etf`;
-- SELECT stage, COUNT(*) n FROM `stonks-498420.stonks_data.stage_engine_v2_etf`
-- GROUP BY stage ORDER BY n DESC;
-- Latest substage per sector/ETF:
-- SELECT ticker, stage, wks_since FROM `stonks-498420.stonks_data.stage_engine_v2_etf`
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY wk DESC)=1 ORDER BY ticker;
