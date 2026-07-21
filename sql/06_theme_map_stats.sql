CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_map_stats` AS
WITH etf_latest AS (
  SELECT ticker, etf_rs
  FROM `stonks-498420.stonks_data.etf_rs_daily`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
)
SELECT
  tm.sub_theme, tm.main_theme, tm.tickers, tm.etf,
  e.etf_rs AS etf_rs,
  ts.members, ts.theme_rs, ts.avg_group_score, ts.group_cohesion,
  mom.theme_rs_today, mom.theme_rs_1d, mom.theme_rs_3d, mom.theme_rs_7d, mom.theme_rs_14d,
  mom.theme_rs_1d_3d, mom.theme_rs_3d_7d, mom.theme_rs_7d_14d, mom.theme_rs_wow, mom.theme_abs_rs_14d,
  mom.theme_rs_trend, mom.theme_bs_ratio_over_ma, mom.theme_bt_ma_rising, mom.theme_pctile_7d,
  mom.theme_today_pctile, mom.strength_in_weakness,
  mom.theme_rs_chg_1d, mom.theme_rs_chg_1w, mom.theme_rs_chg_1m, mom.theme_rs_chg_3m, mom.theme_rs_chg_6m, mom.theme_rs_chg_12m,
  mom.theme_avg_dollar_vol,
  mom.theme_flow_1d, mom.theme_flow_1w, mom.theme_flow_1m, mom.theme_flow_3m, mom.theme_flow_6m, mom.theme_flow_12m,
  mom.theme_flow_day_1d, mom.theme_flow_day_7d, mom.theme_flow_day_30d, mom.theme_flow_day_90d, mom.theme_flow_day_180d, mom.theme_flow_day_364d,
  mom.theme_flow_accel_1dv7d, mom.theme_flow_accel_7dv30d, mom.theme_flow_accel_30dv90d, mom.theme_flow_accel_90dv180d, mom.theme_flow_accel_180dv364d,
  mom.theme_flow_pctile_1d, mom.theme_flow_pctile_7d, mom.theme_flow_pctile_30d, mom.theme_flow_pctile_90d, mom.theme_flow_pctile_180d, mom.theme_flow_pctile_364d,
  mom.etf_rank_today, mom.etf_rank_chg_7d, mom.etf_rank_chg_30d, mom.etf_rank_chg_90d, mom.etf_rank_chg_180d, mom.etf_rank_chg_364d
FROM `stonks-498420.stonks_data.theme_map` tm
LEFT JOIN `stonks-498420.stonks_data.theme_stats` ts        ON ts.theme = tm.sub_theme
LEFT JOIN etf_latest e                                       ON e.ticker = tm.etf
LEFT JOIN `stonks-498420.stonks_data.theme_rs_score_momentum` mom ON mom.theme = tm.sub_theme;
