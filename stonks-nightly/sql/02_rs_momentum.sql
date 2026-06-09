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

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.rs_momentum` AS
WITH spy AS (
  SELECT date, adj_close AS spy_close
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= '2026-01-01' AND ticker='SPY'
),
rat AS (
  SELECT p.ticker, p.date, p.adj_close/s.spy_close AS ratio
  FROM `stonks-498420.stonks_data.price_history` p
  JOIN spy s USING (date)
  WHERE p.date >= '2026-01-01'
),
arr AS (
  SELECT ticker, ARRAY_AGG(ratio ORDER BY date DESC) AS r
  FROM rat GROUP BY ticker
),
mom AS (
  SELECT a.ticker, a.r,
    a.r[OFFSET(0)]    AS rs_today,
    slopecorr(a.r,2)  AS rs_1d,
    slopecorr(a.r,3)  AS rs_3d,
    slopecorr(a.r,7)  AS rs_7d,
    slopecorr(a.r,14) AS rs_14d,
    avgslice(a.r,0,21) AS ratio_ma21,
    avgslice(a.r,1,21) AS ratio_ma21_prior,
    SAFE_DIVIDE(a.r[SAFE_OFFSET(0)], a.r[SAFE_OFFSET(5)]) - 1  AS week1_rel,
    SAFE_DIVIDE(a.r[SAFE_OFFSET(5)], a.r[SAFE_OFFSET(10)]) - 1 AS week2_rel,
    m.rs_value_21d    AS rs_anchor
  FROM arr a
  LEFT JOIN `stonks-498420.stonks_data.metrics_daily` m USING (ticker)
),
calc AS (
  SELECT ticker,
    ROUND(rs_today,4) rs_today,
    ROUND(rs_1d,4) rs_1d, ROUND(rs_3d,4) rs_3d, ROUND(rs_7d,4) rs_7d, ROUND(rs_14d,4) rs_14d,
    ROUND((rs_1d-rs_3d)/NULLIF(rs_3d,0),4) rs_1d_3d,
    ROUND((rs_3d-rs_7d)/NULLIF(rs_7d,0),4) rs_3d_7d,
    ROUND((rs_7d-rs_14d)/NULLIF(rs_14d,0),4) rs_7d_14d,
    ROUND(week1_rel/NULLIF(week2_rel,0),4) rs_wow,
    ROUND(week1_rel - week2_rel,4)         rs_wow_diff,
    CASE WHEN ticker='SPY' THEN 0 ELSE ROUND(ABS(rs_14d*100),4) END AS abs_rs_14d,
    (rs_today > ratio_ma21)            AS bs_ratio_over_ma,
    (ratio_ma21 > ratio_ma21_prior)    AS bt_ma_rising,
    CASE WHEN ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY rs_1d)*100,0)     END AS pctile_1d,
    CASE WHEN ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY rs_3d)*100,0)     END AS pctile_3d,
    CASE WHEN ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY rs_7d)*100,0)     END AS pctile_7d,
    CASE WHEN ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY rs_anchor)*100,0) END AS anchor_pctile,
    CASE WHEN ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY rs_today)*100,0)  END AS today_pctile
  FROM mom
)
SELECT *, GREATEST(pctile_1d, pctile_3d) AS short_pctile
FROM calc;
