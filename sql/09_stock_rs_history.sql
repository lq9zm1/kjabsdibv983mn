CREATE OR REPLACE TABLE `stonks-498420.stonks_data.stock_rs_history`
PARTITION BY DATE_TRUNC(date, MONTH) AS
WITH spy AS (
  SELECT date, adj_close AS spy_close
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= '2025-06-01' AND date < CURRENT_DATE() AND ticker='SPY'
),
joined AS (
  SELECT p.ticker, p.date,
    p.adj_close / NULLIF(s.spy_close,0) AS rs_ratio
  FROM `stonks-498420.stonks_data.price_history` p
  JOIN spy s USING (date)
  WHERE p.date >= '2025-06-01' AND p.date < CURRENT_DATE()
)
SELECT ticker, date, ROUND(rs_ratio,6) AS rs_ratio
FROM joined;
