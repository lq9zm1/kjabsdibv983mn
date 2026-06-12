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
  mom.strength_in_weakness,                -- Y  Col25
  mom.theme_rs_chg_1d,                     -- Z  Col26
  mom.theme_rs_chg_1w,                     -- AA Col27
  mom.theme_rs_chg_1m,                     -- AB Col28
  mom.theme_rs_chg_3m,                     -- AC Col29
  mom.theme_rs_chg_6m,                     -- AD Col30
  mom.theme_rs_chg_12m,                    -- AE Col31
  mom.theme_avg_dollar_vol,                -- AF Col32
  mom.theme_flow_1d,                       -- AG Col33
  mom.theme_flow_1w,                       -- AH Col34
  mom.theme_flow_1m,                       -- AI Col35
  mom.theme_flow_3m,                       -- AJ Col36
  mom.theme_flow_6m,                       -- AK Col37
  mom.theme_flow_12m,                      -- AL Col38
  mom.theme_flow_day_1d,                   -- AM Col39
  mom.theme_flow_day_7d,                   -- AN Col40
  mom.theme_flow_day_30d,                  -- AO Col41
  mom.theme_flow_day_90d,                  -- AP Col42
  mom.theme_flow_day_180d,                 -- AQ Col43
  mom.theme_flow_day_364d,                 -- AR Col44
  mom.theme_flow_accel_1dv7d,              -- AS Col45
  mom.theme_flow_accel_7dv30d,             -- AT Col46
  mom.theme_flow_accel_30dv90d,            -- AU Col47
  mom.theme_flow_accel_90dv180d,           -- AV Col48
  mom.theme_flow_accel_180dv364d,          -- AW Col49
  mom.theme_flow_pctile_1d,                -- AX Col50
  mom.theme_flow_pctile_7d,                -- AY Col51
  mom.theme_flow_pctile_30d,               -- AZ Col52
  mom.theme_flow_pctile_90d,               -- BA Col53
  mom.theme_flow_pctile_180d,              -- BB Col54
  mom.theme_flow_pctile_364d,              -- BC Col55
  mom.etf_rank_today,                      -- BD Col56
  mom.etf_rank_chg_7d,                     -- BE Col57
  mom.etf_rank_chg_30d,                    -- BF Col58
  mom.etf_rank_chg_90d,                    -- BG Col59
  mom.etf_rank_chg_180d,                   -- BH Col60
  mom.etf_rank_chg_364d                    -- BI Col61
FROM `stonks-498420.stonks_data.theme_map` tm
LEFT JOIN `stonks-498420.stonks_data.theme_stats` ts
  ON ts.theme = tm.sub_theme
LEFT JOIN etf_latest e
  ON e.ticker = tm.etf
LEFT JOIN `stonks-498420.stonks_data.theme_rs_score_momentum` mom
  ON mom.theme = tm.sub_theme;
