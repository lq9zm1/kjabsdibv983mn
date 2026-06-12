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
    s.spy_ret_1d
  FROM piv p
  LEFT JOIN spy s ON s.date = p.date
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
  ( spy_ret_1d < 0 AND
    CASE
      WHEN (ma21 > ma21_prior) AND theme_rs_7d > 0 THEN 'Rising'
      WHEN (ma21 <= ma21_prior) AND theme_rs_7d < 0 THEN 'Falling'
      WHEN theme_rs_14d IS NULL THEN NULL ELSE 'Flat'
    END IN ('Rising','Flat')
  ) AS strength_in_weakness
FROM calc;
