-- ============================================================================
-- 05b_etf_rs_daily — ETF + SECTOR relative strength, computed IN BQ from etf_prices.
-- Fixes theme_map_stats.etf_rs (was null: it joined ETFs to metrics_daily, which is stocks only).
-- RS = percentile of 21-day RS-vs-SPY, ranked WITHIN universe:
--   • the 11 SPDR sectors ranked among sectors      (your "Sector Data" tab)
--   • category='etf' industry/thematic ETFs among ETFs (your "ETF Data" tab)
-- SPY (benchmark) comes from price_history (always pulled by run_nightly). Sectors ARE in
-- etf_prices already (etf_universe.category='sector'), so no separate sector_prices needed.
-- Output: ticker, date, rank_grp, rs21 (raw 21d RS-vs-SPY), etf_rs (0–1 percentile = RS_STS).
-- Historical (month-partitioned) → feeds theme_map_stats AND the leader engine's etf-strength.
-- Runs after price_history (nightly step 3) and after pull_etfs refreshes etf_prices.
-- ============================================================================
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.etf_rs_daily`
PARTITION BY DATE_TRUNC(date, MONTH) CLUSTER BY ticker AS
WITH
u AS ( SELECT ticker, category FROM `stonks-498420.stonks_data.etf_universe` ),
epx AS (                                   -- etf_prices.date is TIMESTAMP -> cast to DATE to match price_history
  SELECT p.ticker, DATE(p.date) AS date,
    p.adj_close / NULLIF(LAG(p.adj_close,21) OVER (PARTITION BY p.ticker ORDER BY p.date),0) AS r21
  FROM `stonks-498420.stonks_data.etf_prices` p
),
spy AS (                                   -- cast to DATE too (defensive; keeps USING(date) type-safe)
  SELECT DATE(date) AS date, adj_close / NULLIF(LAG(adj_close,21) OVER (ORDER BY date),0) AS spy_r21
  FROM `stonks-498420.stonks_data.price_history` WHERE ticker='SPY'
),
rel AS (
  SELECT e.ticker, e.date, u.category,
    (e.r21 / NULLIF(s.spy_r21,0)) - 1 AS rs21
  FROM epx e
  JOIN u   USING (ticker)
  JOIN spy s USING (date)
  WHERE e.r21 IS NOT NULL AND s.spy_r21 IS NOT NULL
),
grp AS (
  SELECT ticker, date, category, rs21,
    CASE
      WHEN ticker IN ('XLK','XLF','XLE','XLI','XLB','XLV','XLY','XLP','XLU','XLRE','XLC') THEN 'sector'
      WHEN category = 'etf' THEN 'etf'
      ELSE NULL          -- macro + non-SPDR sector-tagged excluded from the RS rank
    END AS rank_grp
  FROM rel
)
SELECT ticker, date, rank_grp, ROUND(rs21,6) AS rs21,
  ROUND(PERCENT_RANK() OVER (PARTITION BY date, rank_grp ORDER BY rs21), 4) AS etf_rs
FROM grp
WHERE rank_grp IS NOT NULL;
