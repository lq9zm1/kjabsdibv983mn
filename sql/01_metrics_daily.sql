CREATE TEMP FUNCTION ema(arr ARRAY<FLOAT64>, n FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if(!arr || arr.length < n) return null;
  var k = 2/(n+1);
  // seed = SMA of first n values (TradingView ta.ema convention)
  var seed = 0;
  for (var i = 0; i < n; i++) seed += arr[i];
  var e = seed / n;
  // recurse from index n onward
  for (var j = n; j < arr.length; j++) { e = arr[j]*k + e*(1-k); }
  return e;
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
)
SELECT
  ticker, date, close,
  ret_1d, ret_1w, ret_1m, ret_3m, ret_6m, ret_12m,
  rs_value_21d,
  ROUND((RANK() OVER (ORDER BY rs_value_21d)-1)*100.0/NULLIF(COUNT(*) OVER()-1,0),0) AS rs_pctile,
  sma_5, sma_10, sma_20, sma_50, sma_200,
  ema_5, ema_10, ema_21,
  px_vs_50sma, px_vs_200sma,
  atr_pct, adr_pct,
  ROUND(px_vs_50sma*100/NULLIF(atr_pct,0),4) AS atr_extension,
  avg_dollar_vol,
  ROUND(avg_dollar_vol/NULLIF(atr_pct,0),0) AS dvol_per_atr,
  ret_3d, ret_14d, ret_30d, ret_d1_7, ret_d8_14,
  IF(rawclose_for_flags > f5,1,0)   AS abv_sma5_eod,
  IF(rawclose_for_flags > f10,1,0)  AS abv_sma10_eod,
  IF(rawclose_for_flags > f20,1,0)  AS abv_sma20_eod,
  IF(rawclose_for_flags > f50,1,0)  AS abv_sma50_eod,
  IF(rawclose_for_flags > f200,1,0) AS abv_sma200_eod,
  IF(rawclose_for_flags > fe5,1,0)  AS abv_ema5_eod,
  IF(rawclose_for_flags > fe10,1,0) AS abv_ema10_eod,
  IF(rawclose_for_flags > fe21,1,0) AS abv_ema21_eod
FROM joined;
