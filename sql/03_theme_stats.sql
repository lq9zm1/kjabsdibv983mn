CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_stats` AS
WITH per_stock AS (
  SELECT s.sub_theme,
    m.rs_value_21d AS stock_theme_rs,
    m.dvol_per_atr,
    (SELECT AVG(r) FROM UNNEST([m.ret_1d,m.ret_1w,m.ret_1m,m.ret_3m,m.ret_6m]) r) AS group_score
  FROM `stonks-498420.stonks_data.stock_theme_map` s
  JOIN `stonks-498420.stonks_data.metrics_daily` m USING (ticker)
),
agg AS (
  SELECT sub_theme AS theme,
    COUNT(*) AS members,
    AVG(dvol_per_atr)        AS avg_dvol_atr,
    STDDEV_SAMP(dvol_per_atr) AS std_dev,
    AVG(stock_theme_rs)      AS theme_rs_avg,
    AVG(group_score)         AS avg_group_score,
    STDDEV_SAMP(group_score) AS ranking_spread
  FROM per_stock GROUP BY sub_theme
),
rets AS (
  SELECT s.sub_theme, p.ticker, p.date,
         SAFE_DIVIDE(p.adj_close, LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date)) - 1 AS ret,
         ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.date DESC) AS rn
  FROM `stonks-498420.stonks_data.stock_theme_map` s
  JOIN `stonks-498420.stonks_data.price_history` p USING (ticker)
  WHERE p.date >= '2025-11-01' AND p.date < CURRENT_DATE()
),
win AS ( SELECT sub_theme, ticker, date, ret FROM rets WHERE rn <= 63 AND ret IS NOT NULL ),
pair_corr AS (
  SELECT a.sub_theme, a.ticker t1, b.ticker t2, CORR(a.ret, b.ret) AS c
  FROM win a
  JOIN win b ON a.sub_theme=b.sub_theme AND a.date=b.date AND a.ticker < b.ticker
  GROUP BY 1,2,3
  HAVING c IS NOT NULL AND IS_NAN(c) = FALSE AND ABS(c) <= 1
),
cohesion AS (
  SELECT sub_theme AS theme, ROUND(AVG(c),3) AS group_cohesion
  FROM pair_corr GROUP BY sub_theme
)
SELECT
  a.theme, a.members,
  ROUND(a.avg_dvol_atr,2) AS avg_dvol_atr,
  ROUND(a.std_dev,2)      AS std_dev,
  ROUND(PERCENT_RANK() OVER (ORDER BY a.theme_rs_avg) * 100, 0) AS theme_rs,
  ROUND(a.avg_group_score,4) AS avg_group_score,
  ROUND(a.ranking_spread,4)  AS ranking_spread,
  ROUND(SAFE_DIVIDE(a.ranking_spread,a.avg_group_score),4) AS spread_from_avg,
  CASE WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.3 THEN 0.4
       WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.6 THEN 0.6
       WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<1   THEN 0.8 ELSE 1 END AS sensitivity_mult,
  ROUND(a.avg_group_score + a.ranking_spread *
    CASE WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.3 THEN 0.4
         WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.6 THEN 0.6
         WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<1 THEN 0.8 ELSE 1 END, 4) AS upper_range,
  ROUND(a.avg_group_score - a.ranking_spread *
    CASE WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.3 THEN 0.4
         WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<0.6 THEN 0.6
         WHEN SAFE_DIVIDE(a.ranking_spread,a.avg_group_score)<1 THEN 0.8 ELSE 1 END, 4) AS lower_range,
  co.group_cohesion
FROM agg a
LEFT JOIN cohesion co ON co.theme = a.theme;
