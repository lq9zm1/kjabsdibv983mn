CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_map_stats` AS
SELECT
  tm.sub_theme,
  tm.main_theme,
  tm.tickers,
  tm.etf,
  e.rs_value_21d        AS etf_rs,
  ts.members,
  ts.theme_rs,
  ts.avg_group_score,
  ts.group_cohesion
FROM `stonks-498420.stonks_data.theme_map` tm
LEFT JOIN `stonks-498420.stonks_data.theme_stats` ts
  ON ts.theme = tm.sub_theme
LEFT JOIN `stonks-498420.stonks_data.metrics_daily` e
  ON e.ticker = tm.etf;
