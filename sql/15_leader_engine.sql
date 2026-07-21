-- ============================================================================
-- 15_leader_engine — historical 3-leader engine (theme/universe/etf) + RS + EXT.
-- Per (ticker,date) over the full price_history universe. Self-contained; nightly-safe.
--   theme_leader   = composite (group_score=AVG(ret_1d,1w,1m,3m,6m)) ranked WITHIN sub-theme
--   universe_leader= cross-sectional pctile of the composite across ALL stocks (0–100)
--   etf_leader     = stock 1M ret − its ETF's 1M ret (pct-pts; >0 = leading its ETF)
--   rs_rsp         = Leader-Hunter RS (percentrank of price/RSP over trailing 22) — computed INLINE
--                    (needs RSP in etf_prices; add RSP to etf_universe so pull_etfs fetches it).
-- Runs after price_history (nightly step 3). Also emits v_leaders (latest-per-ticker snapshot).
-- ============================================================================
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.bt_leader_daily`
PARTITION BY DATE_TRUNC(date, MONTH) CLUSTER BY ticker AS
WITH
px AS (
  SELECT ticker, DATE(date) AS date, adj_close, high, low,
    LAG(adj_close)     OVER w AS prev_c, LAG(adj_close,5)   OVER w AS c5,
    LAG(adj_close,21)  OVER w AS c21,    LAG(adj_close,63)  OVER w AS c63,
    LAG(adj_close,126) OVER w AS c126,
    AVG(adj_close) OVER (w ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50
  FROM `stonks-498420.stonks_data.price_history`
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
trc AS ( SELECT *, GREATEST(high-low, ABS(high-prev_c), ABS(low-prev_c)) AS tr1 FROM px ),
atr AS ( SELECT *, AVG(tr1) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr14 FROM trc ),
ret AS (
  SELECT ticker, date, adj_close, sma50, atr14,
    adj_close/NULLIF(prev_c,0)-1 AS ret_1d, adj_close/NULLIF(c5,0)-1  AS ret_1w,
    adj_close/NULLIF(c21,0)-1    AS ret_1m, adj_close/NULLIF(c63,0)-1 AS ret_3m,
    adj_close/NULLIF(c126,0)-1   AS ret_6m
  FROM atr
),
comp AS (
  SELECT ticker, date, adj_close, sma50, atr14, ret_1m, ret_3m, ret_6m,
    (SELECT AVG(x) FROM UNNEST([ret_1d,ret_1w,ret_1m,ret_3m,ret_6m]) x) AS group_score,
    SAFE_DIVIDE(adj_close - sma50, sma50) / NULLIF(atr14/NULLIF(adj_close,0),0) AS ext
  FROM ret
),
uni AS (
  SELECT ticker, date, group_score, ret_1m, ext,
    ROUND(100*PERCENT_RANK() OVER (PARTITION BY date ORDER BY group_score),1) AS universe_leader,
    ROUND(100*PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret_1m),1)     AS uni_1m_pct,
    ROUND(100*PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret_3m),1)     AS uni_3m_pct,
    ROUND(100*PERCENT_RANK() OVER (PARTITION BY date ORDER BY ret_6m),1)     AS uni_6m_pct
  FROM comp WHERE group_score IS NOT NULL
),
thm AS (
  SELECT c.ticker, c.date, stm.sub_theme,
    ROUND(100*PERCENT_RANK() OVER (PARTITION BY c.date, stm.sub_theme ORDER BY c.group_score),1) AS theme_leader_pct,
    RANK() OVER (PARTITION BY c.date, stm.sub_theme ORDER BY c.group_score DESC) AS theme_leader_rank
  FROM comp c JOIN `stonks-498420.stonks_data.stock_theme_map` stm ON stm.ticker = c.ticker
  WHERE c.group_score IS NOT NULL
),
thm1 AS (
  SELECT ticker, date, sub_theme, theme_leader_pct, theme_leader_rank
  FROM thm QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY theme_leader_pct DESC) = 1
),
etfp AS (
  SELECT ticker AS etf, DATE(date) AS date,
    adj_close/NULLIF(LAG(adj_close,21) OVER (PARTITION BY ticker ORDER BY date),0)-1 AS etf_ret_1m
  FROM `stonks-498420.stonks_data.etf_prices`
),
smap AS ( SELECT ticker, ANY_VALUE(etf) AS etf FROM `stonks-498420.stonks_data.stock_theme_map` WHERE etf IS NOT NULL GROUP BY ticker ),
etfl AS (
  SELECT u.ticker, u.date, ROUND(100*(u.ret_1m - e.etf_ret_1m),2) AS etf_leader
  FROM uni u JOIN smap m ON m.ticker=u.ticker JOIN etfp e ON e.etf=m.etf AND e.date=u.date
),
-- rs_rsp computed INLINE (percentrank of price/RSP over trailing 22) — needs RSP in etf_prices
rspx AS ( SELECT DATE(date) AS date, adj_close AS rsp_close FROM `stonks-498420.stonks_data.etf_prices` WHERE ticker='RSP' ),
rsr0 AS ( SELECT c.ticker, c.date, c.adj_close/NULLIF(r.rsp_close,0) AS pr FROM comp c JOIN rspx r ON r.date=c.date ),
rsr AS (
  SELECT ticker, date,
    ROUND((SELECT COUNTIF(v<pr) FROM UNNEST(w) v)/NULLIF(ARRAY_LENGTH(w)-1,0),4) AS rs_rsp
  FROM ( SELECT ticker, date, pr, ARRAY_AGG(pr) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 21 PRECEDING AND CURRENT ROW) AS w FROM rsr0 )
  WHERE ARRAY_LENGTH(w) >= 22
)
SELECT
  u.ticker, u.date,
  ROUND(u.group_score,4) AS group_score, ROUND(u.ext,2) AS ext,
  t.sub_theme, t.theme_leader_pct, t.theme_leader_rank,
  u.universe_leader, u.uni_1m_pct, u.uni_3m_pct, u.uni_6m_pct,
  el.etf_leader,
  rr.rs_rsp
FROM uni u
LEFT JOIN thm1 t  ON t.ticker=u.ticker AND t.date=u.date
LEFT JOIN etfl el ON el.ticker=u.ticker AND el.date=u.date
LEFT JOIN rsr  rr ON rr.ticker=u.ticker AND rr.date=u.date;

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leaders` AS
SELECT l.*, tk.name, tk.sector, tk.industry
FROM `stonks-498420.stonks_data.bt_leader_daily` l
LEFT JOIN `stonks-498420.stonks_data.tickers` tk USING (ticker)
QUALIFY ROW_NUMBER() OVER (PARTITION BY l.ticker ORDER BY l.date DESC) = 1;
