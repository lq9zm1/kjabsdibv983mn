CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_map_stats` AS
WITH etf_latest AS (
  SELECT ticker, rs_value_21d
  FROM `stonks-498420.stonks_data.metrics_daily`
  WHERE date < CURRENT_DATE()
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
)
SELECT
  tm.sub_theme,                            -- A  Col1
  tm.main_theme,                           -- B  Col2
  tm.tickers,                              -- C  Col3
  tm.etf,                                  -- D  Col4
  e.rs_value_21d            AS etf_rs,      -- E  Col5
  ts.members,                              -- F  Col6
  ts.theme_rs,                             -- G  Col7
  ts.avg_group_score,                      -- H  Col8
  ts.group_cohesion,                       -- I  Col9
  mom.theme_rs_today,                      -- J  Col10
  mom.theme_rs_1d,                         -- K  Col11
  mom.theme_rs_3d,                         -- L  Col12
  mom.theme_rs_7d,                         -- M  Col13
  mom.theme_rs_14d,                        -- N  Col14
  mom.theme_rs_1d_3d,                      -- O  Col15
  mom.theme_rs_3d_7d,                      -- P  Col16
  mom.theme_rs_7d_14d,                     -- Q  Col17
  mom.theme_rs_wow,                        -- R  Col18
  mom.theme_abs_rs_14d,                    -- S  Col19
  mom.theme_rs_trend,                      -- T  Col20
  mom.theme_bs_ratio_over_ma,              -- U  Col21
  mom.theme_bt_ma_rising,                  -- V  Col22
  mom.theme_pctile_7d,                     -- W  Col23
  mom.theme_today_pctile,                  -- X  Col24
  mom.strength_in_weakness                 -- Y  Col25
FROM `stonks-498420.stonks_data.theme_map` tm
LEFT JOIN `stonks-498420.stonks_data.theme_stats` ts
  ON ts.theme = tm.sub_theme
LEFT JOIN etf_latest e
  ON e.ticker = tm.etf
LEFT JOIN `stonks-498420.stonks_data.theme_rs_score_momentum` mom
  ON mom.theme = tm.sub_theme;
