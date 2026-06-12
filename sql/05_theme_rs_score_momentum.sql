CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_rs_score_momentum` AS
WITH h AS (
  SELECT
    theme, date, theme_rs,
    ROW_NUMBER() OVER (PARTITION BY theme ORDER BY date DESC) AS rn,
    AVG(theme_rs) OVER (PARTITION BY theme ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)  AS ma21,
    AVG(theme_rs) OVER (PARTITION BY theme ORDER BY date ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING)  AS ma21_prior
  FROM `stonks-498420.stonks_data.theme_rs_score_history`
),
piv AS (
  SELECT
    theme,
    MAX(IF(rn=1,   date, NULL))        AS date,
    MAX(IF(rn=1,   theme_rs, NULL))    AS rs_today,
    MAX(IF(rn=2,   theme_rs, NULL))    AS rs_1ago,
    MAX(IF(rn=4,   theme_rs, NULL))    AS rs_3ago,
    MAX(IF(rn=8,   theme_rs, NULL))    AS rs_7ago,
    MAX(IF(rn=15,  theme_rs, NULL))    AS rs_14ago,
    MAX(IF(rn=6,   theme_rs, NULL))    AS rs_5ago,
    MAX(IF(rn=11,  theme_rs, NULL))    AS rs_10ago,
    MAX(IF(rn=22,  theme_rs, NULL))    AS rs_21ago,
    MAX(IF(rn=64,  theme_rs, NULL))    AS rs_63ago,
    MAX(IF(rn=127, theme_rs, NULL))    AS rs_126ago,
    MAX(IF(rn=253, theme_rs, NULL))    AS rs_252ago,
    MAX(IF(rn=1,   ma21, NULL))        AS ma21,
    MAX(IF(rn=1,   ma21_prior, NULL))  AS ma21_prior
  FROM h
  WHERE rn <= 253
  GROUP BY theme
),
spy AS (
  SELECT
    date,
    adj_close / NULLIF(LAG(adj_close) OVER (ORDER BY date),0) - 1 AS spy_ret_1d
  FROM `stonks-498420.stonks_data.price_history`
  WHERE ticker='SPY' AND date < CURRENT_DATE()
  QUALIFY date = (SELECT MAX(date) FROM piv)
),
flow AS (
  SELECT
    s.sub_theme AS theme,
    AVG(m.avg_dollar_vol) AS theme_avg_dollar_vol,
    AVG(m.ret_1d)  AS avg_ret_1d,
    AVG(m.ret_1w)  AS avg_ret_1w,
    AVG(m.ret_1m)  AS avg_ret_1m,
    AVG(m.ret_3m)  AS avg_ret_3m,
    AVG(m.ret_6m)  AS avg_ret_6m,
    AVG(m.ret_12m) AS avg_ret_12m
  FROM `stonks-498420.stonks_data.stock_theme_map` s
  JOIN `stonks-498420.stonks_data.metrics_daily` m USING (ticker)
  GROUP BY s.sub_theme
),
etf_map AS (
  SELECT sub_theme AS theme, etf
  FROM `stonks-498420.stonks_data.theme_map`
  WHERE etf IS NOT NULL AND TRIM(etf) <> ''
),
etf_sizes AS (
  SELECT etf
  FROM etf_map
  GROUP BY etf
  HAVING COUNT(*) > 1
),
etf_rank_hist AS (                         -- 0-100 percentile WITHIN ETF per date (100=leads ETF)
  SELECT
    hh.theme, hh.date,
    PERCENT_RANK() OVER (PARTITION BY em.etf, hh.date ORDER BY hh.theme_rs ASC) * 100 AS etf_pct,
    ROW_NUMBER() OVER (PARTITION BY hh.theme ORDER BY hh.date DESC) AS rn
  FROM `stonks-498420.stonks_data.theme_rs_score_history` hh
  JOIN etf_map em ON em.theme = hh.theme
  JOIN etf_sizes es ON es.etf = em.etf
),
etf_rank_piv AS (
  SELECT
    theme,
    MAX(IF(rn=1,   etf_pct, NULL)) AS etf_pct_today,
    MAX(IF(rn=6,   etf_pct, NULL)) AS etf_pct_5ago,
    MAX(IF(rn=22,  etf_pct, NULL)) AS etf_pct_21ago,
    MAX(IF(rn=64,  etf_pct, NULL)) AS etf_pct_63ago,
    MAX(IF(rn=127, etf_pct, NULL)) AS etf_pct_126ago,
    MAX(IF(rn=253, etf_pct, NULL)) AS etf_pct_252ago
  FROM etf_rank_hist
  WHERE rn <= 253
  GROUP BY theme
),
calc AS (
  SELECT
    p.theme, p.date,
    p.rs_today                                            AS theme_rs_today,
    p.rs_today - p.rs_1ago                                AS theme_rs_1d,
    p.rs_today - p.rs_3ago                                AS theme_rs_3d,
    p.rs_today - p.rs_7ago                                AS theme_rs_7d,
    p.rs_today - p.rs_14ago                               AS theme_rs_14d,
    (p.rs_today - p.rs_1ago) - (p.rs_today - p.rs_3ago)   AS theme_rs_1d_3d,
    (p.rs_today - p.rs_3ago) - (p.rs_today - p.rs_7ago)   AS theme_rs_3d_7d,
    (p.rs_today - p.rs_7ago) - (p.rs_today - p.rs_14ago)  AS theme_rs_7d_14d,
    (p.rs_today - p.rs_5ago)                              AS week1_delta,
    (p.rs_5ago  - p.rs_10ago)                             AS week2_delta,
    p.rs_today - p.rs_1ago                                AS theme_rs_chg_1d,
    p.rs_today - p.rs_5ago                                AS theme_rs_chg_1w,
    p.rs_today - p.rs_21ago                               AS theme_rs_chg_1m,
    p.rs_today - p.rs_63ago                               AS theme_rs_chg_3m,
    p.rs_today - p.rs_126ago                              AS theme_rs_chg_6m,
    p.rs_today - p.rs_252ago                              AS theme_rs_chg_12m,
    p.ma21, p.ma21_prior,
    s.spy_ret_1d,
    f.theme_avg_dollar_vol,
    f.theme_avg_dollar_vol * f.avg_ret_1d  AS theme_flow_1d,
    f.theme_avg_dollar_vol * f.avg_ret_1w  AS theme_flow_1w,
    f.theme_avg_dollar_vol * f.avg_ret_1m  AS theme_flow_1m,
    f.theme_avg_dollar_vol * f.avg_ret_3m  AS theme_flow_3m,
    f.theme_avg_dollar_vol * f.avg_ret_6m  AS theme_flow_6m,
    f.theme_avg_dollar_vol * f.avg_ret_12m AS theme_flow_12m,
    (f.theme_avg_dollar_vol * f.avg_ret_1d)  / 1   AS theme_flow_day_1d,
    (f.theme_avg_dollar_vol * f.avg_ret_1w)  / 7   AS theme_flow_day_7d,
    (f.theme_avg_dollar_vol * f.avg_ret_1m)  / 30  AS theme_flow_day_30d,
    (f.theme_avg_dollar_vol * f.avg_ret_3m)  / 90  AS theme_flow_day_90d,
    (f.theme_avg_dollar_vol * f.avg_ret_6m)  / 180 AS theme_flow_day_180d,
    (f.theme_avg_dollar_vol * f.avg_ret_12m) / 364 AS theme_flow_day_364d,
    er.etf_pct_today,
    er.etf_pct_today - er.etf_pct_5ago    AS etf_rank_chg_7d,
    er.etf_pct_today - er.etf_pct_21ago   AS etf_rank_chg_30d,
    er.etf_pct_today - er.etf_pct_63ago   AS etf_rank_chg_90d,
    er.etf_pct_today - er.etf_pct_126ago  AS etf_rank_chg_180d,
    er.etf_pct_today - er.etf_pct_252ago  AS etf_rank_chg_364d
  FROM piv p
  LEFT JOIN spy  s ON s.date = p.date
  LEFT JOIN flow f ON f.theme = p.theme
  LEFT JOIN etf_rank_piv er ON er.theme = p.theme
)
SELECT
  theme, date,
  ROUND(theme_rs_today,2)  AS theme_rs_today,
  ROUND(theme_rs_1d,2)     AS theme_rs_1d,
  ROUND(theme_rs_3d,2)     AS theme_rs_3d,
  ROUND(theme_rs_7d,2)     AS theme_rs_7d,
  ROUND(theme_rs_14d,2)    AS theme_rs_14d,
  ROUND(theme_rs_1d_3d,2)  AS theme_rs_1d_3d,
  ROUND(theme_rs_3d_7d,2)  AS theme_rs_3d_7d,
  ROUND(theme_rs_7d_14d,2) AS theme_rs_7d_14d,
  ROUND(week1_delta - week2_delta,2) AS theme_rs_wow,
  ROUND(week1_delta - week2_delta,2) AS theme_rs_wow_diff,
  ROUND(ABS(theme_rs_14d),2)         AS theme_abs_rs_14d,
  (theme_rs_today > ma21)            AS theme_bs_ratio_over_ma,
  (ma21 > ma21_prior)                AS theme_bt_ma_rising,
  CASE
    WHEN (ma21 > ma21_prior) AND theme_rs_7d > 0 THEN 'Rising'
    WHEN (ma21 <= ma21_prior) AND theme_rs_7d < 0 THEN 'Falling'
    WHEN theme_rs_14d IS NULL THEN NULL ELSE 'Flat'
  END AS theme_rs_trend,
  ROUND(spy_ret_1d,4)                AS spy_ret_1d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_rs_1d)*100,0)    AS theme_pctile_1d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_rs_3d)*100,0)    AS theme_pctile_3d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_rs_7d)*100,0)    AS theme_pctile_7d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_rs_14d)*100,0)   AS theme_pctile_14d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_rs_today)*100,0) AS theme_today_pctile,
  ROUND(theme_rs_chg_1d,2)  AS theme_rs_chg_1d,
  ROUND(theme_rs_chg_1w,2)  AS theme_rs_chg_1w,
  ROUND(theme_rs_chg_1m,2)  AS theme_rs_chg_1m,
  ROUND(theme_rs_chg_3m,2)  AS theme_rs_chg_3m,
  ROUND(theme_rs_chg_6m,2)  AS theme_rs_chg_6m,
  ROUND(theme_rs_chg_12m,2) AS theme_rs_chg_12m,
  ROUND(theme_avg_dollar_vol,0) AS theme_avg_dollar_vol,
  ROUND(theme_flow_1d,0)  AS theme_flow_1d,
  ROUND(theme_flow_1w,0)  AS theme_flow_1w,
  ROUND(theme_flow_1m,0)  AS theme_flow_1m,
  ROUND(theme_flow_3m,0)  AS theme_flow_3m,
  ROUND(theme_flow_6m,0)  AS theme_flow_6m,
  ROUND(theme_flow_12m,0) AS theme_flow_12m,
  ROUND(theme_flow_day_1d,0)   AS theme_flow_day_1d,
  ROUND(theme_flow_day_7d,0)   AS theme_flow_day_7d,
  ROUND(theme_flow_day_30d,0)  AS theme_flow_day_30d,
  ROUND(theme_flow_day_90d,0)  AS theme_flow_day_90d,
  ROUND(theme_flow_day_180d,0) AS theme_flow_day_180d,
  ROUND(theme_flow_day_364d,0) AS theme_flow_day_364d,
  ROUND(theme_flow_day_1d   - theme_flow_day_7d,   0) AS theme_flow_accel_1dv7d,
  ROUND(theme_flow_day_7d   - theme_flow_day_30d,  0) AS theme_flow_accel_7dv30d,
  ROUND(theme_flow_day_30d  - theme_flow_day_90d,  0) AS theme_flow_accel_30dv90d,
  ROUND(theme_flow_day_90d  - theme_flow_day_180d, 0) AS theme_flow_accel_90dv180d,
  ROUND(theme_flow_day_180d - theme_flow_day_364d, 0) AS theme_flow_accel_180dv364d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_1d)*100,0)  AS theme_flow_pctile_1d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_1w)*100,0)  AS theme_flow_pctile_7d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_1m)*100,0)  AS theme_flow_pctile_30d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_3m)*100,0)  AS theme_flow_pctile_90d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_6m)*100,0)  AS theme_flow_pctile_180d,
  ROUND(PERCENT_RANK() OVER (ORDER BY theme_flow_12m)*100,0) AS theme_flow_pctile_364d,
  ROUND(etf_pct_today,0)      AS etf_rank_today,
  ROUND(etf_rank_chg_7d,0)    AS etf_rank_chg_7d,
  ROUND(etf_rank_chg_30d,0)   AS etf_rank_chg_30d,
  ROUND(etf_rank_chg_90d,0)   AS etf_rank_chg_90d,
  ROUND(etf_rank_chg_180d,0)  AS etf_rank_chg_180d,
  ROUND(etf_rank_chg_364d,0)  AS etf_rank_chg_364d,
  ( spy_ret_1d < 0 AND
    CASE
      WHEN (ma21 > ma21_prior) AND theme_rs_7d > 0 THEN 'Rising'
      WHEN (ma21 <= ma21_prior) AND theme_rs_7d < 0 THEN 'Falling'
      WHEN theme_rs_14d IS NULL THEN NULL ELSE 'Flat'
    END IN ('Rising','Flat')
  ) AS strength_in_weakness
FROM calc;
