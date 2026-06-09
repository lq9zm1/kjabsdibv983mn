CREATE TEMP FUNCTION slopecorr(arr ARRAY<FLOAT64>, n FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if(!arr || arr.length < n) return null;
  var N=n, sx=0, sy=0, sxy=0, sxx=0, syy=0;
  for(var i=0;i<N;i++){ var x=i+1, y=arr[i]; sx+=x; sy+=y; sxy+=x*y; sxx+=x*x; syy+=y*y; }
  var cov=sxy - sx*sy/N, vx=sxx - sx*sx/N, vy=syy - sy*sy/N;
  if(vx<=0||vy<=0) return null;
  return (cov/vx)*(cov/Math.sqrt(vx*vy))*100;
""";
CREATE TEMP FUNCTION avgslice(arr ARRAY<FLOAT64>, startIdx FLOAT64, cnt FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if(!arr) return null; var s=0,n=0;
  for(var i=startIdx;i<startIdx+cnt && i<arr.length;i++){s+=arr[i];n++;}
  return n>0 ? s/n : null;
""";

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_rs_momentum` AS
WITH member_ret AS (
  SELECT s.sub_theme, p.ticker, p.date,
    p.adj_close/NULLIF(LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date),0)-1 AS ret
  FROM `stonks-498420.stonks_data.stock_theme_map` s
  JOIN `stonks-498420.stonks_data.price_history` p USING (ticker)
  WHERE p.date >= '2026-01-01'
),
theme_ret AS (
  SELECT sub_theme, date, AVG(ret) AS tret
  FROM member_ret WHERE ret IS NOT NULL
  GROUP BY sub_theme, date
),
theme_idx AS (
  SELECT sub_theme, date,
    EXP(SUM(LN(1+tret)) OVER (PARTITION BY sub_theme ORDER BY date)) AS tidx
  FROM theme_ret WHERE tret > -1
),
spy_ret AS (
  SELECT date, adj_close/NULLIF(LAG(adj_close) OVER (ORDER BY date),0)-1 AS sret
  FROM `stonks-498420.stonks_data.price_history`
  WHERE ticker='SPY' AND date >= '2026-01-01'
),
spy_idx AS (
  SELECT date, EXP(SUM(LN(1+sret)) OVER (ORDER BY date)) AS sidx
  FROM spy_ret WHERE sret > -1
),
rat AS (
  SELECT t.sub_theme, t.date, t.tidx/NULLIF(s.sidx,0) AS ratio
  FROM theme_idx t JOIN spy_idx s USING (date)
),
arr AS (
  SELECT sub_theme, ARRAY_AGG(ratio ORDER BY date DESC) AS r
  FROM rat GROUP BY sub_theme
),
mom AS (
  SELECT sub_theme,
    r[OFFSET(0)] AS rs_today,
    slopecorr(r,2) rs_1d, slopecorr(r,3) rs_3d, slopecorr(r,7) rs_7d, slopecorr(r,14) rs_14d,
    avgslice(r,0,21) ma21, avgslice(r,1,21) ma21_prior,
    SAFE_DIVIDE(r[SAFE_OFFSET(0)], r[SAFE_OFFSET(5)]) - 1  AS week1_rel,
    SAFE_DIVIDE(r[SAFE_OFFSET(5)], r[SAFE_OFFSET(10)]) - 1 AS week2_rel
  FROM arr
),
calc AS (
  SELECT sub_theme AS theme,
    ROUND(rs_today,4) theme_rs_today,
    ROUND(rs_1d,4) theme_rs_1d, ROUND(rs_3d,4) theme_rs_3d,
    ROUND(rs_7d,4) theme_rs_7d, ROUND(rs_14d,4) theme_rs_14d,
    ROUND((rs_1d-rs_3d)/NULLIF(rs_3d,0),4) theme_rs_1d_3d,
    ROUND((rs_3d-rs_7d)/NULLIF(rs_7d,0),4) theme_rs_3d_7d,
    ROUND((rs_7d-rs_14d)/NULLIF(rs_14d,0),4) theme_rs_7d_14d,
    ROUND(week1_rel/NULLIF(week2_rel,0),4) theme_rs_wow,
    ROUND(week1_rel - week2_rel,4)         theme_rs_wow_diff,
    ROUND(ABS(rs_14d*100),4) theme_abs_rs_14d,
    (rs_today > ma21) theme_bs_ratio_over_ma,
    (ma21 > ma21_prior) theme_bt_ma_rising,
    CASE
      WHEN (ma21 > ma21_prior) AND ((rs_7d-rs_14d)/NULLIF(rs_14d,0)) > 0 THEN 'Rising'
      WHEN (ma21 <= ma21_prior) AND ((rs_7d-rs_14d)/NULLIF(rs_14d,0)) < 0 THEN 'Falling'
      WHEN rs_14d IS NULL THEN NULL
      ELSE 'Flat'
    END AS theme_rs_trend,
    ROUND(CUME_DIST() OVER (ORDER BY rs_1d)*100,0)    theme_pctile_1d,
    ROUND(CUME_DIST() OVER (ORDER BY rs_3d)*100,0)    theme_pctile_3d,
    ROUND(CUME_DIST() OVER (ORDER BY rs_7d)*100,0)    theme_pctile_7d,
    ROUND(CUME_DIST() OVER (ORDER BY rs_14d)*100,0)   theme_pctile_14d,
    ROUND(CUME_DIST() OVER (ORDER BY rs_today)*100,0) theme_today_pctile
  FROM mom
)
SELECT *, GREATEST(theme_pctile_1d, theme_pctile_3d) AS theme_short_pctile
FROM calc;
