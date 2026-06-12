CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_rs_score_history`
PARTITION BY date
CLUSTER BY theme AS
WITH pr AS (
  SELECT
    ticker, date, adj_close,
    adj_close / NULLIF(LAG(adj_close,20) OVER (PARTITION BY ticker ORDER BY date),0) AS rs_ret20
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
    AND date <  CURRENT_DATE()
),
spy_d AS (
  SELECT date, rs_ret20 AS spy_ret
  FROM pr
  WHERE ticker = 'SPY'
),
raw AS (
  SELECT
    pr.ticker, pr.date,
    ROUND((pr.rs_ret20 / NULLIF(spy_d.spy_ret,0) - 1) * 100, 2) AS raw_rs
  FROM pr
  JOIN spy_d USING (date)
  WHERE pr.rs_ret20 IS NOT NULL AND spy_d.spy_ret IS NOT NULL
),
pctd AS (
  SELECT
    date,
    APPROX_QUANTILES(raw_rs, 100)[OFFSET(1)]  AS p1,
    APPROX_QUANTILES(raw_rs, 100)[OFFSET(99)] AS p99
  FROM raw
  GROUP BY date
),
sdd AS (
  SELECT
    r.date,
    STDDEV(LEAST(GREATEST(r.raw_rs, p.p1), p.p99)) AS sd
  FROM raw r
  JOIN pctd p USING (date)
  GROUP BY r.date
),
scored AS (
  SELECT
    r.ticker, r.date,
    CASE WHEN r.ticker = 'SPY' THEN 50
         ELSE ROUND(100 / (1 + EXP(-( LEAST(GREATEST(r.raw_rs, p.p1), p.p99) / NULLIF(d.sd,0) ))), 0)
    END AS rs_value_21d
  FROM raw r
  JOIN pctd p USING (date)
  JOIN sdd  d USING (date)
),
theme_day AS (
  SELECT
    m.sub_theme AS theme,
    s.date,
    AVG(s.rs_value_21d) AS theme_rs_avg
  FROM scored s
  JOIN `stonks-498420.stonks_data.stock_theme_map` m USING (ticker)
  GROUP BY m.sub_theme, s.date
)
SELECT
  theme,
  date,
  ROUND(theme_rs_avg, 2) AS theme_rs_avg,
  ROUND(PERCENT_RANK() OVER (PARTITION BY date ORDER BY theme_rs_avg) * 100, 0) AS theme_rs
FROM theme_day;
