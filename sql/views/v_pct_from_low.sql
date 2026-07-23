-- sql/views/v_pct_from_low.sql — pct_from_low at 3M/6M/12M (6M canonical), adj_close-based (split-safe), to 1962.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_pct_from_low` AS
WITH lows AS (
  SELECT ticker, DATE(date) AS date, adj_close,
    MIN(adj_close) OVER w63  AS lo_3m,   COUNT(*) OVER w63  AS n_3m,
    MIN(adj_close) OVER w126 AS lo_6m,   COUNT(*) OVER w126 AS n_6m,
    MIN(adj_close) OVER w252 AS lo_12m,  COUNT(*) OVER w252 AS n_12m
  FROM `stonks-498420.stonks_data.price_history`
  WINDOW
    w63  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 62  PRECEDING AND CURRENT ROW),   -- PARAM 3M  = 63
    w126 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW),   -- PARAM 6M  = 126 (canonical)
    w252 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)    -- PARAM 12M = 252
)
SELECT ticker, date, adj_close, lo_3m, lo_6m, lo_12m, n_3m, n_6m, n_12m,
  SAFE_DIVIDE(adj_close - lo_3m,  lo_3m)  AS pct_from_low_3m,
  SAFE_DIVIDE(adj_close - lo_6m,  lo_6m)  AS pct_from_low_6m,
  SAFE_DIVIDE(adj_close - lo_12m, lo_12m) AS pct_from_low_12m
FROM lows;
