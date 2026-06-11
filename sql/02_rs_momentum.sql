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
  WHERE date >= '2026-01-01' AND date < CURRENT_DATE() AND ticker='SPY'
),
rat AS (
  SELECT p.ticker, p.date, p.adj_close/s.spy_close AS ratio
  FROM `stonks-498420.stonks_data.price_history` p
  JOIN spy s USING (date)
  WHERE p.date >= '2026-01-01' AND p.date < CURRENT_DATE()
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
raw AS (
  SELECT ticker,
    rs_today,
    rs_1d, rs_3d, rs_7d, rs_14d,
    (rs_1d - rs_3d)/NULLIF(rs_3d,0)   AS rs_1d_3d,
    (rs_3d - rs_7d)/NULLIF(rs_7d,0)   AS rs_3d_7d,
    (rs_7d - rs_14d)/NULLIF(rs_14d,0) AS rs_7d_14d,
    week1_rel/NULLIF(week2_rel,0)     AS rs_wow,
    week1_rel - week2_rel             AS rs_wow_diff,
    ratio_ma21, ratio_ma21_prior, rs_anchor
  FROM mom
),
stats AS (
  SELECT
    APPROX_QUANTILES(rs_today,100)[OFFSET(1)]    AS p1_today,  APPROX_QUANTILES(rs_today,100)[OFFSET(99)]    AS p99_today,
    APPROX_QUANTILES(rs_1d,100)[OFFSET(1)]       AS p1_1d,     APPROX_QUANTILES(rs_1d,100)[OFFSET(99)]       AS p99_1d,
    APPROX_QUANTILES(rs_3d,100)[OFFSET(1)]       AS p1_3d,     APPROX_QUANTILES(rs_3d,100)[OFFSET(99)]       AS p99_3d,
    APPROX_QUANTILES(rs_7d,100)[OFFSET(1)]       AS p1_7d,     APPROX_QUANTILES(rs_7d,100)[OFFSET(99)]       AS p99_7d,
    APPROX_QUANTILES(rs_14d,100)[OFFSET(1)]      AS p1_14d,    APPROX_QUANTILES(rs_14d,100)[OFFSET(99)]      AS p99_14d,
    APPROX_QUANTILES(rs_wow,100)[OFFSET(1)]      AS p1_wow,    APPROX_QUANTILES(rs_wow,100)[OFFSET(99)]      AS p99_wow,
    APPROX_QUANTILES(rs_wow_diff,100)[OFFSET(1)] AS p1_wowd,   APPROX_QUANTILES(rs_wow_diff,100)[OFFSET(99)] AS p99_wowd
  FROM raw
),
sd AS (
  SELECT
    STDDEV(LEAST(GREATEST(r.rs_today,    st.p1_today), st.p99_today))  AS sd_today,
    STDDEV(LEAST(GREATEST(r.rs_1d,       st.p1_1d),    st.p99_1d))     AS sd_1d,
    STDDEV(LEAST(GREATEST(r.rs_3d,       st.p1_3d),    st.p99_3d))     AS sd_3d,
    STDDEV(LEAST(GREATEST(r.rs_7d,       st.p1_7d),    st.p99_7d))     AS sd_7d,
    STDDEV(LEAST(GREATEST(r.rs_14d,      st.p1_14d),   st.p99_14d))    AS sd_14d,
    STDDEV(LEAST(GREATEST(r.rs_wow,      st.p1_wow),   st.p99_wow))    AS sd_wow,
    STDDEV(LEAST(GREATEST(r.rs_wow_diff, st.p1_wowd),  st.p99_wowd))   AS sd_wowd
  FROM raw r CROSS JOIN stats st
),
spy_rank AS (
  SELECT
    MAX(IF(ticker='SPY', cd_1d3d, NULL))  AS spy_1d3d,
    MAX(IF(ticker='SPY', cd_3d7d, NULL))  AS spy_3d7d,
    MAX(IF(ticker='SPY', cd_7d14d, NULL)) AS spy_7d14d
  FROM (
    SELECT ticker,
      CUME_DIST() OVER (ORDER BY rs_1d_3d)  *100 AS cd_1d3d,
      CUME_DIST() OVER (ORDER BY rs_3d_7d)  *100 AS cd_3d7d,
      CUME_DIST() OVER (ORDER BY rs_7d_14d) *100 AS cd_7d14d
    FROM raw
  )
),
ranked AS (
  SELECT ticker,
    CUME_DIST() OVER (ORDER BY rs_1d_3d)  *100 AS cd_1d3d,
    CUME_DIST() OVER (ORDER BY rs_3d_7d)  *100 AS cd_3d7d,
    CUME_DIST() OVER (ORDER BY rs_7d_14d) *100 AS cd_7d14d
  FROM raw
),
final AS (
  SELECT
    r.ticker,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( (LEAST(GREATEST(r.rs_today, st.p1_today), st.p99_today) - 1.0) / NULLIF(sd.sd_today,0) ))),0) END AS rs_today,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_1d,  st.p1_1d),  st.p99_1d)  / NULLIF(sd.sd_1d,0) ))),0) END AS rs_1d,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_3d,  st.p1_3d),  st.p99_3d)  / NULLIF(sd.sd_3d,0) ))),0) END AS rs_3d,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_7d,  st.p1_7d),  st.p99_7d)  / NULLIF(sd.sd_7d,0) ))),0) END AS rs_7d,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_14d, st.p1_14d), st.p99_14d) / NULLIF(sd.sd_14d,0) ))),0) END AS rs_14d,
    CASE WHEN r.ticker='SPY' THEN 50
         ELSE ROUND(LEAST(GREATEST(rk.cd_1d3d + (50 - sr.spy_1d3d), 0), 100),0) END AS rs_1d_3d,
    CASE WHEN r.ticker='SPY' THEN 50
         ELSE ROUND(LEAST(GREATEST(rk.cd_3d7d + (50 - sr.spy_3d7d), 0), 100),0) END AS rs_3d_7d,
    CASE WHEN r.ticker='SPY' THEN 50
         ELSE ROUND(LEAST(GREATEST(rk.cd_7d14d + (50 - sr.spy_7d14d), 0), 100),0) END AS rs_7d_14d,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_wow,    st.p1_wow),    st.p99_wow)    / NULLIF(sd.sd_wow,0) ))),0) END AS rs_wow,
    CASE WHEN r.ticker='SPY' THEN 50 ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(r.rs_wow_diff, st.p1_wowd),  st.p99_wowd)  / NULLIF(sd.sd_wowd,0) ))),0) END AS rs_wow_diff,
    CASE WHEN r.ticker='SPY' THEN 0 ELSE ROUND(ABS(r.rs_14d*100),4) END AS abs_rs_14d,
    (r.rs_today > r.ratio_ma21)         AS bs_ratio_over_ma,
    (r.ratio_ma21 > r.ratio_ma21_prior) AS bt_ma_rising,
    CASE WHEN r.ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY r.rs_1d)*100,0)     END AS pctile_1d,
    CASE WHEN r.ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY r.rs_3d)*100,0)     END AS pctile_3d,
    CASE WHEN r.ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY r.rs_7d)*100,0)     END AS pctile_7d,
    CASE WHEN r.ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY r.rs_anchor)*100,0) END AS anchor_pctile,
    CASE WHEN r.ticker='SPY' THEN 100 ELSE ROUND(CUME_DIST() OVER (ORDER BY r.rs_today)*100,0)  END AS today_pctile
  FROM raw r
  CROSS JOIN stats st
  CROSS JOIN sd
  CROSS JOIN spy_rank sr
  JOIN ranked rk USING (ticker)
)
SELECT *, GREATEST(pctile_1d, pctile_3d) AS short_pctile
FROM final;
