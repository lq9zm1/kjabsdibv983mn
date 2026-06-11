CREATE TEMP FUNCTION ema(arr ARRAY<FLOAT64>, n FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if(!arr || arr.length < n) return null;
  var k = 2/(n+1);
  var seed = 0;
  for (var i = 0; i < n; i++) seed += arr[i];
  var e = seed / n;
  for (var j = n; j < arr.length; j++) { e = arr[j]*k + e*(1-k); }
  return e;
""";

CREATE TEMP FUNCTION wilder_atr(tr ARRAY<FLOAT64>, n FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if(!tr||tr.length<n) return null;
  var seed=0; for(var i=0;i<n;i++) seed+=tr[i]; seed/=n;
  var a=seed;
  for(var j=n;j<tr.length;j++){ a=(a*(n-1)+tr[j])/n; }
  return a;
""";

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.metrics_daily` AS
WITH base AS (
  SELECT ticker, date, high, low, adj_close, volume,
         LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date) AS prev_adj
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= '2024-01-01' AND date < CURRENT_DATE()
),
perrow AS (
  SELECT ticker, date, adj_close,
    adj_close/NULLIF(LAG(adj_close,1)   OVER w,0)-1 AS ret_1d,
    adj_close/NULLIF(LAG(adj_close,5)   OVER w,0)-1 AS ret_1w,
    adj_close/NULLIF(LAG(adj_close,21)  OVER w,0)-1 AS ret_1m,
    adj_close/NULLIF(LAG(adj_close,63)  OVER w,0)-1 AS ret_3m,
    adj_close/NULLIF(LAG(adj_close,126) OVER w,0)-1 AS ret_6m,
    adj_close/NULLIF(LAG(adj_close,252) OVER w,0)-1 AS ret_12m,
    adj_close/NULLIF(LAG(adj_close,3)   OVER w,0)-1 AS ret_3d,
    adj_close/NULLIF(LAG(adj_close,14)  OVER w,0)-1 AS ret_14d,
    adj_close/NULLIF(LAG(adj_close,30)  OVER w,0)-1 AS ret_30d,
    adj_close/NULLIF(LAG(adj_close,7)   OVER w,0)-1 AS ret_d1_7,
    LAG(adj_close,7) OVER w/NULLIF(LAG(adj_close,14) OVER w,0)-1 AS ret_d8_14,
    adj_close/NULLIF(LAG(adj_close,20)  OVER w,0)   AS rs_ret20,
    AVG(adj_close) OVER (w ROWS BETWEEN 4   PRECEDING AND CURRENT ROW) AS sma5,
    AVG(adj_close) OVER (w ROWS BETWEEN 9   PRECEDING AND CURRENT ROW) AS sma10,
    AVG(adj_close) OVER (w ROWS BETWEEN 19  PRECEDING AND CURRENT ROW) AS sma20,
    AVG(adj_close) OVER (w ROWS BETWEEN 49  PRECEDING AND CURRENT ROW) AS sma50,
    AVG(adj_close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200,
    AVG(volume)    OVER (w ROWS BETWEEN 19  PRECEDING AND CURRENT ROW) AS vol20,
    (AVG(high/NULLIF(low,0)) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)-1)*100 AS adr_pct,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
  FROM base
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
latest AS (SELECT * FROM perrow WHERE rn=1),
arr AS (
  SELECT ticker,
    ARRAY_AGG(adj_close ORDER BY date) AS adj_arr,
    ARRAY_AGG(GREATEST(high-low, ABS(high-prev_adj), ABS(low-prev_adj)) IGNORE NULLS ORDER BY date) AS tr_arr
  FROM base GROUP BY ticker
),
spy AS (SELECT rs_ret20 AS spy_ret FROM latest WHERE ticker='SPY'),
joined AS (
  SELECT l.ticker, l.date,
    ROUND(l.adj_close,2) AS close,
    ROUND(l.ret_1d,4) ret_1d, ROUND(l.ret_1w,4) ret_1w, ROUND(l.ret_1m,4) ret_1m,
    ROUND(l.ret_3m,4) ret_3m, ROUND(l.ret_6m,4) ret_6m, ROUND(l.ret_12m,4) ret_12m,
    ROUND(l.ret_3d,4) ret_3d, ROUND(l.ret_14d,4) ret_14d, ROUND(l.ret_30d,4) ret_30d,
    ROUND(l.ret_d1_7,4) ret_d1_7, ROUND(l.ret_d8_14,4) ret_d8_14,
    ROUND((l.rs_ret20/s.spy_ret-1)*100,2) AS rs_value_21d,
    ROUND(l.sma5,2) sma_5, ROUND(l.sma10,2) sma_10, ROUND(l.sma20,2) sma_20,
    ROUND(l.sma50,2) sma_50, ROUND(l.sma200,2) sma_200,
    ema(a.adj_arr,5)  ema_5,
    ema(a.adj_arr,10) ema_10,
    ema(a.adj_arr,21) ema_21,
    ROUND(l.adj_close/NULLIF(l.sma50,0)-1,4)  AS px_vs_50sma,
    ROUND(l.adj_close/NULLIF(l.sma200,0)-1,4) AS px_vs_200sma,
    ROUND(wilder_atr(a.tr_arr,14)/NULLIF(l.adj_close,0)*100,4) AS atr_pct,
    ROUND(l.adr_pct,4) AS adr_pct,
    ROUND(l.adj_close*l.vol20,0) AS avg_dollar_vol,
    l.adj_close AS rawclose_for_flags,
    l.sma5 f5, l.sma10 f10, l.sma20 f20, l.sma50 f50, l.sma200 f200,
    ema(a.adj_arr,5) fe5, ema(a.adj_arr,10) fe10, ema(a.adj_arr,21) fe21
  FROM latest l CROSS JOIN spy s
  LEFT JOIN arr a USING (ticker)
),
pct AS (
  SELECT
    APPROX_QUANTILES(rs_value_21d, 100)[OFFSET(1)]  AS p1,
    APPROX_QUANTILES(rs_value_21d, 100)[OFFSET(99)] AS p99
  FROM joined
),
s AS (
  SELECT p.p1, p.p99,
    STDDEV(LEAST(GREATEST(j.rs_value_21d, p.p1), p.p99)) AS sd
  FROM joined j CROSS JOIN pct p
  GROUP BY p.p1, p.p99
)
SELECT
  j.ticker, j.date, j.close,
  j.ret_1d, j.ret_1w, j.ret_1m, j.ret_3m, j.ret_6m, j.ret_12m,
  CASE WHEN j.ticker='SPY' THEN 50
       ELSE ROUND(100/(1+EXP(-( LEAST(GREATEST(j.rs_value_21d, s.p1), s.p99) / NULLIF(s.sd,0) ))),0)
  END AS rs_value_21d,
  ROUND((RANK() OVER (ORDER BY j.rs_value_21d)-1)*100.0/NULLIF(COUNT(*) OVER()-1,0),0) AS rs_pctile,
  j.sma_5, j.sma_10, j.sma_20, j.sma_50, j.sma_200,
  j.ema_5, j.ema_10, j.ema_21,
  j.px_vs_50sma, j.px_vs_200sma,
  j.atr_pct, j.adr_pct,
  ROUND(j.px_vs_50sma*100/NULLIF(j.atr_pct,0),4) AS atr_extension,
  j.avg_dollar_vol,
  ROUND(j.avg_dollar_vol/NULLIF(j.atr_pct,0),0) AS dvol_per_atr,
  j.ret_3d, j.ret_14d, j.ret_30d, j.ret_d1_7, j.ret_d8_14,
  IF(j.rawclose_for_flags > j.f5,1,0)   AS abv_sma5_eod,
  IF(j.rawclose_for_flags > j.f10,1,0)  AS abv_sma10_eod,
  IF(j.rawclose_for_flags > j.f20,1,0)  AS abv_sma20_eod,
  IF(j.rawclose_for_flags > j.f50,1,0)  AS abv_sma50_eod,
  IF(j.rawclose_for_flags > j.f200,1,0) AS abv_sma200_eod,
  IF(j.rawclose_for_flags > j.fe5,1,0)  AS abv_ema5_eod,
  IF(j.rawclose_for_flags > j.fe10,1,0) AS abv_ema10_eod,
  IF(j.rawclose_for_flags > j.fe21,1,0) AS abv_ema21_eod
FROM joined j CROSS JOIN s;
