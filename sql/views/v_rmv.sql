-- sql/views/v_rmv.sql — Relative Measured Volatility (0-100) per (ticker,date). Pure OHLC, to 1962.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_rmv` AS
WITH base AS (
  SELECT ticker, DATE(date) AS date, (high - low) AS rng
  FROM `stonks-498420.stonks_data.price_history`
  WHERE high IS NOT NULL AND low IS NOT NULL AND high >= low
),
env AS (
  SELECT ticker, date, rng,
    MIN(rng) OVER w AS rmin, MAX(rng) OVER w AS rmax, COUNT(rng) OVER w AS n_env
  FROM base
  WINDOW w AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING)   -- PARAM ENV_WIN=14
),
calc AS (
  SELECT ticker, date, rng,
    CASE WHEN n_env >= 14
         THEN LEAST(100, GREATEST(0, 100 * SAFE_DIVIDE(rng - rmin, NULLIF(rmax - rmin, 0)))) END AS rmv
  FROM env
),
lagged AS (
  SELECT ticker, date, rng, rmv, LAG(rmv,1) OVER wt AS rmv_l1, LAG(rmv,2) OVER wt AS rmv_l2
  FROM calc WINDOW wt AS (PARTITION BY ticker ORDER BY date)
)
SELECT ticker, date, rng, rmv, rmv_l1, rmv_l2,
  (rmv < 15 AND rmv_l1 < 15 AND rmv_l2 >= 15) AS rmv_tight   -- PARAM tight_T=15 (x2); loose_gate=15
FROM lagged;
