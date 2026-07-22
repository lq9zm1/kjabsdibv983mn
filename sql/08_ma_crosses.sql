CREATE TEMP FUNCTION ema_series(arr ARRAY<FLOAT64>, n FLOAT64)
RETURNS ARRAY<FLOAT64> LANGUAGE js AS r"""
  if(!arr||!arr.length) return [];
  var k=2/(n+1), out=[], e=arr[0];
  out.push(e);
  for(var i=1;i<arr.length;i++){ e=arr[i]*k + e*(1-k); out.push(e); }
  return out;
""";
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.ma_crosses` AS
WITH px AS (
  SELECT ticker, date, adj_close,
    AVG(adj_close) OVER (w ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)   AS sma5,
    AVG(adj_close) OVER (w ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)   AS sma10,
    AVG(adj_close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)  AS sma20,
    AVG(adj_close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma50,
    AVG(adj_close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY) AND date < CURRENT_DATE()
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
arr AS (
  SELECT ticker,
    ARRAY_AGG(date ORDER BY date) AS dates,
    ARRAY_AGG(adj_close ORDER BY date) AS closes
  FROM px GROUP BY ticker
),
ema_arrays AS (
  SELECT ticker, dates,
    ema_series(closes, 5)  AS e5,
    ema_series(closes, 10) AS e10,
    ema_series(closes, 21) AS e21
  FROM arr
),
ema_calc AS (
  SELECT a.ticker, dt AS date,
    a.e5[OFFSET(o)]  AS ema5,
    a.e10[OFFSET(o)] AS ema10,
    a.e21[OFFSET(o)] AS ema21
  FROM ema_arrays a, UNNEST(a.dates) AS dt WITH OFFSET o
),
joined AS (
  SELECT p.ticker, p.date, p.adj_close,
    p.sma5, p.sma10, p.sma20, p.sma50, p.sma200,
    e.ema5, e.ema10, e.ema21
  FROM px p
  JOIN ema_calc e ON e.ticker = p.ticker AND e.date = p.date
),
flagged AS (
  SELECT ticker, date,
    COUNT(*) OVER (PARTITION BY ticker) AS hist_len,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn,
    IF(adj_close>sma5,1,0) a5,  IF(adj_close>sma10,1,0) a10, IF(adj_close>sma20,1,0) a20,
    IF(adj_close>sma50,1,0) a50, IF(adj_close>sma200,1,0) a200,
    IF(adj_close>ema5,1,0) e5,  IF(adj_close>ema10,1,0) e10, IF(adj_close>ema21,1,0) e21,
    LAG(IF(adj_close>sma5,1,0))   OVER w p5,   LAG(IF(adj_close>sma10,1,0))  OVER w p10,
    LAG(IF(adj_close>sma20,1,0))  OVER w p20,  LAG(IF(adj_close>sma50,1,0))  OVER w p50,
    LAG(IF(adj_close>sma200,1,0)) OVER w p200,
    LAG(IF(adj_close>ema5,1,0))   OVER w pe5,  LAG(IF(adj_close>ema10,1,0))  OVER w pe10,
    LAG(IF(adj_close>ema21,1,0))  OVER w pe21
  FROM joined
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
)
SELECT ticker,
  COALESCE(MIN(IF(a5  != p5,  rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_sma5,
  ANY_VALUE(IF(rn=1, IF(a5=1,'Above','Below'), NULL))   AS cross_dir_sma5,
  COALESCE(MIN(IF(a10 != p10, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_sma10,
  ANY_VALUE(IF(rn=1, IF(a10=1,'Above','Below'), NULL))  AS cross_dir_sma10,
  COALESCE(MIN(IF(a20 != p20, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_sma20,
  ANY_VALUE(IF(rn=1, IF(a20=1,'Above','Below'), NULL))  AS cross_dir_sma20,
  COALESCE(MIN(IF(a50 != p50, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_sma50,
  ANY_VALUE(IF(rn=1, IF(a50=1,'Above','Below'), NULL))  AS cross_dir_sma50,
  COALESCE(MIN(IF(a200 != p200, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_sma200,
  ANY_VALUE(IF(rn=1, IF(a200=1,'Above','Below'), NULL)) AS cross_dir_sma200,
  COALESCE(MIN(IF(e5  != pe5,  rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_ema5,
  ANY_VALUE(IF(rn=1, IF(e5=1,'Above','Below'), NULL))   AS cross_dir_ema5,
  COALESCE(MIN(IF(e10 != pe10, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_ema10,
  ANY_VALUE(IF(rn=1, IF(e10=1,'Above','Below'), NULL))  AS cross_dir_ema10,
  COALESCE(MIN(IF(e21 != pe21, rn, NULL)), ANY_VALUE(hist_len)) AS days_since_cross_ema21,
  ANY_VALUE(IF(rn=1, IF(e21=1,'Above','Below'), NULL))  AS cross_dir_ema21
FROM flagged
GROUP BY ticker;
