-- ============================================================================
-- STONKS: theme_rs_history_deep — ONE-TIME deep backfill (1993 -> now). TABLE ONLY.
-- Mirrors sql/10_theme_rs_history.sql EXACTLY, floors 2024-06/2025-06 -> 1993-01-01,
-- output -> theme_rs_history_deep. SINGLE PASS (cumulative theme index + 40-day self-join
-- CANNOT be year-chunked). Uses current stock_theme_map membership (same as nightly).
-- ⚠️ HEAVY (self-join of the 40-day ratio window over full history + JS UDFs). One-time / MANUAL.
--    If it errors on quota/time, tell me and I'll bound it. Nightly sql/10 is UNTOUCHED.
--
-- ⚠️ The union VIEW is NOT in this file. BigQuery forbids creating a view in the same script that
--    defines temporary UDFs (the slopecorr/avgslice below), so the view lives in its own file:
--    run  v_theme_rs_history_full.sql  ONCE, separately, after this table build. (The view is
--    idempotent CREATE OR REPLACE — a table rebuild does not require re-running it.)
-- Run in BigQuery Studio (paste all, Run).
-- ============================================================================

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

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.theme_rs_history_deep`
PARTITION BY DATE_TRUNC(date, MONTH) AS
WITH member_ret AS (
  SELECT s.sub_theme, p.ticker, p.date,
    p.adj_close/NULLIF(LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date),0)-1 AS ret
  FROM `stonks-498420.stonks_data.stock_theme_map` s
  JOIN `stonks-498420.stonks_data.price_history` p USING (ticker)
  WHERE p.date >= '1993-01-01' AND p.date < CURRENT_DATE()
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
  WHERE ticker='SPY' AND date >= '1993-01-01' AND date < CURRENT_DATE()
),
spy_idx AS (
  SELECT date, EXP(SUM(LN(1+sret)) OVER (ORDER BY date)) AS sidx
  FROM spy_ret WHERE sret > -1
),
rat AS (
  SELECT t.sub_theme, t.date, t.tidx/NULLIF(s.sidx,0) AS ratio
  FROM theme_idx t JOIN spy_idx s USING (date)
),
windowed AS (
  SELECT a.sub_theme, a.date AS anchor_date,
    ARRAY_AGG(b.ratio ORDER BY b.date DESC) AS r
  FROM rat a
  JOIN rat b ON b.sub_theme=a.sub_theme AND b.date <= a.date AND b.date > DATE_SUB(a.date, INTERVAL 40 DAY)
  WHERE a.date >= '1993-01-01'
  GROUP BY a.sub_theme, a.date
),
mom AS (
  SELECT sub_theme, anchor_date AS date,
    r[OFFSET(0)] AS rs_today,
    slopecorr(r,2) rs_1d, slopecorr(r,3) rs_3d, slopecorr(r,7) rs_7d, slopecorr(r,14) rs_14d,
    avgslice(r,0,21) ma21, avgslice(r,1,21) ma21_prior,
    SAFE_DIVIDE(r[SAFE_OFFSET(0)], r[SAFE_OFFSET(5)]) - 1  AS week1_rel,
    SAFE_DIVIDE(r[SAFE_OFFSET(5)], r[SAFE_OFFSET(10)]) - 1 AS week2_rel
  FROM windowed
),
calc AS (
  SELECT m.sub_theme AS theme, m.date,
    ROUND(m.rs_today,4) theme_rs_today,
    ROUND(m.rs_1d,4) theme_rs_1d, ROUND(m.rs_3d,4) theme_rs_3d,
    ROUND(m.rs_7d,4) theme_rs_7d, ROUND(m.rs_14d,4) theme_rs_14d,
    ROUND((m.rs_1d-m.rs_3d)/NULLIF(m.rs_3d,0),4) theme_rs_1d_3d,
    ROUND((m.rs_3d-m.rs_7d)/NULLIF(m.rs_7d,0),4) theme_rs_3d_7d,
    ROUND((m.rs_7d-m.rs_14d)/NULLIF(m.rs_14d,0),4) theme_rs_7d_14d,
    ROUND(m.week1_rel/NULLIF(m.week2_rel,0),4) theme_rs_wow,
    ROUND(m.week1_rel - m.week2_rel,4)         theme_rs_wow_diff,
    ROUND(ABS(m.rs_14d*100),4) theme_abs_rs_14d,
    (m.rs_today > m.ma21) theme_bs_ratio_over_ma,
    (m.ma21 > m.ma21_prior) theme_bt_ma_rising,
    CASE
      WHEN (m.ma21 > m.ma21_prior) AND ((m.rs_7d-m.rs_14d)/NULLIF(m.rs_14d,0)) > 0 THEN 'Rising'
      WHEN (m.ma21 <= m.ma21_prior) AND ((m.rs_7d-m.rs_14d)/NULLIF(m.rs_14d,0)) < 0 THEN 'Falling'
      WHEN m.rs_14d IS NULL THEN NULL ELSE 'Flat'
    END AS theme_rs_trend,
    ROUND(sr.sret,4) AS spy_ret_1d,
    ROUND(CUME_DIST() OVER (PARTITION BY m.date ORDER BY m.rs_1d)*100,0)    theme_pctile_1d,
    ROUND(CUME_DIST() OVER (PARTITION BY m.date ORDER BY m.rs_3d)*100,0)    theme_pctile_3d,
    ROUND(CUME_DIST() OVER (PARTITION BY m.date ORDER BY m.rs_7d)*100,0)    theme_pctile_7d,
    ROUND(CUME_DIST() OVER (PARTITION BY m.date ORDER BY m.rs_14d)*100,0)   theme_pctile_14d,
    ROUND(CUME_DIST() OVER (PARTITION BY m.date ORDER BY m.rs_today)*100,0) theme_today_pctile
  FROM mom m
  LEFT JOIN spy_ret sr ON sr.date = m.date
)
SELECT *,
  ( spy_ret_1d < 0 AND theme_rs_trend IN ('Rising','Flat') ) AS strength_in_weakness
FROM calc;

-- verify (table only):
-- SELECT MIN(date) mn, MAX(date) mx, COUNT(*) n FROM `stonks-498420.stonks_data.theme_rs_history_deep`;
-- Then run v_theme_rs_history_full.sql and verify the view.
