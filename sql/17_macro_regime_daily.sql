-- sql/17_macro_regime_daily.sql — NIGHTLY full rebuild (idempotent CREATE OR REPLACE).
-- TIDELINE (v4, 2026-07-26): WEEKLY (Monday) MACD(6/20) vs SMA-9 signal, LIVE forming week (no lag),
--   independent UDF `tide_wk_macd` (no sata_macd). Risk On = weekly MACD > weekly signal.
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.macro_regime_daily`
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY etf AS
WITH
px AS (
  SELECT p.ticker, DATE(p.date) AS date, p.adj_close, p.close, p.high, p.low, u.category
  FROM `stonks-498420.stonks_data.etf_prices` p
  LEFT JOIN `stonks-498420.stonks_data.etf_universe` u USING (ticker)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.ticker, DATE(p.date) ORDER BY p.date DESC) = 1
),
sm AS (
  SELECT ticker, date, category, adj_close, close, high, low,
    AVG(adj_close) OVER w5   AS sma5,
    AVG(adj_close) OVER w10  AS sma10,
    AVG(adj_close) OVER w20  AS sma20,
    AVG(adj_close) OVER w50  AS sma50,
    AVG(adj_close) OVER w200 AS sma200,
    AVG(close)     OVER w50  AS sma50_raw,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) - 1 AS k
  FROM px
  WINDOW
    w5   AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4   PRECEDING AND CURRENT ROW),
    w10  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9   PRECEDING AND CURRENT ROW),
    w20  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19  PRECEDING AND CURRENT ROW),
    w50  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49  PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
),
sm2 AS (SELECT *, LAG(sma20) OVER (PARTITION BY ticker ORDER BY date) AS sma20_prev FROM sm),

-- ===== NEW weekly Tideline (independent MACD line; live forming week; no sata_macd) =====
wkly AS (
  SELECT ticker, DATE_TRUNC(date, WEEK(MONDAY)) AS wk_start,
         ARRAY_AGG(adj_close ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS wclose
  FROM px GROUP BY ticker, wk_start
),
wk_idx AS (SELECT ticker, wk_start, wclose, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY wk_start) AS widx FROM wkly),
wk_arr AS (SELECT ticker, ARRAY_AGG(wclose ORDER BY wk_start) AS wcloses FROM wk_idx GROUP BY ticker),
wk_macd AS (SELECT ticker, `stonks-498420.stonks_data.tide_wk_macd`(wcloses, 6, 20) AS arr FROM wk_arr),
wk_bar AS (
  SELECT ticker, off + 1 AS widx, e.ema_fast AS ema6, e.ema_slow AS ema20, e.macd AS macd
  FROM wk_macd, UNNEST(arr) AS e WITH OFFSET AS off
),
wk_feat AS (
  SELECT ticker, widx, ema6, ema20, macd,
    SUM(macd)   OVER (PARTITION BY ticker ORDER BY widx ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS macd_sum8,
    COUNT(macd) OVER (PARTITION BY ticker ORDER BY widx ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS macd_cnt8
  FROM wk_bar
),
day_wk AS (
  SELECT s.*, i.widx AS day_widx
  FROM sm2 s JOIN wk_idx i ON i.ticker = s.ticker AND i.wk_start = DATE_TRUNC(s.date, WEEK(MONDAY))
),
day_base AS (
  SELECT d.*, f.ema6 AS base_ema6, f.ema20 AS base_ema20, f.macd_sum8 AS base_sum8, f.macd_cnt8 AS base_cnt8
  FROM day_wk d LEFT JOIN wk_feat f ON f.ticker = d.ticker AND f.widx = d.day_widx - 1
),
form AS (
  SELECT db.*,
    CASE WHEN base_ema6 IS NULL OR base_ema20 IS NULL THEN NULL
         ELSE ((2.0/7.0)*adj_close + (5.0/7.0)*base_ema6)
            - ((2.0/21.0)*adj_close + (19.0/21.0)*base_ema20) END AS macd_asof
  FROM day_base db
),
form2 AS (
  SELECT f.*, CASE WHEN macd_asof IS NULL OR base_cnt8 < 8 THEN NULL
                   ELSE (base_sum8 + macd_asof)/9.0 END AS signal_asof
  FROM form f
),
tr AS (
  SELECT ticker, date,
    GREATEST(high - low,
             ABS(high - LAG(close) OVER (PARTITION BY ticker ORDER BY date)),
             ABS(low  - LAG(close) OVER (PARTITION BY ticker ORDER BY date))) AS trv
  FROM px
),
tragg AS (SELECT ticker, ARRAY_AGG(trv ORDER BY date) AS trs FROM tr WHERE trv IS NOT NULL GROUP BY ticker),
atrcalc AS (SELECT ticker, `stonks-498420.stonks_data.rt_ema`(trs, 27) AS aa FROM tragg),
atr_by_bar AS (SELECT ticker, j + 1 AS k, a AS atrv FROM atrcalc, UNNEST(aa) AS a WITH OFFSET AS j)
SELECT
  s.ticker AS etf,
  s.category,
  s.date,
  s.adj_close, s.close,
  s.sma5, s.sma10, s.sma20, s.sma50, s.sma200,
  IF(s.adj_close > s.sma5,   'Above','Below') AS abv_5,
  IF(s.adj_close > s.sma10,  'Above','Below') AS abv_10,
  IF(s.adj_close > s.sma20,  'Above','Below') AS abv_20,
  IF(s.adj_close > s.sma50,  'Above','Below') AS abv_50,
  IF(s.adj_close > s.sma200, 'Above','Below') AS abv_200,
  CASE
    WHEN s.adj_close <= s.sma200 THEN 'Downtrend'
    WHEN s.adj_close <= s.sma50  THEN 'Avoid'
    WHEN s.adj_close <= s.sma20  THEN 'Pullback'
    WHEN s.adj_close <= s.sma10  THEN 'Uptrend'
    WHEN s.adj_close <= s.sma5   THEN 'Strong'
    ELSE 'Exceptional'
  END AS ma_status,
  s.macd_asof   AS tide_macd,
  s.signal_asof AS tide_signal,
  CASE WHEN s.macd_asof IS NULL OR s.signal_asof IS NULL THEN ''
       WHEN s.macd_asof > s.signal_asof THEN 'Risk On' ELSE 'Risk Off' END AS tideline,
  CASE WHEN s.macd_asof IS NULL OR s.signal_asof IS NULL THEN NULL
       ELSE s.macd_asof > s.signal_asof END AS risk_on,
  CASE
    WHEN s.adj_close >  s.sma20 AND s.sma20 >  s.sma20_prev THEN 'Above Rising'
    WHEN s.adj_close >  s.sma20 AND s.sma20 <= s.sma20_prev THEN 'Above Declining'
    WHEN s.adj_close <= s.sma20 AND s.sma20 >  s.sma20_prev THEN 'Below Rising'
    ELSE 'Below Declining'
  END AS market_trend,
  ROUND(SAFE_DIVIDE((SAFE_DIVIDE(s.close, s.sma50_raw) - 1) * s.close, ab.atrv), 2) AS atr_ext
FROM form2 s
LEFT JOIN atr_by_bar ab ON ab.ticker = s.ticker AND ab.k = s.k;
