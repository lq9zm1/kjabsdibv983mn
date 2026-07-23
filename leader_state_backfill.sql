-- ============================================================================
-- leader_state_backfill.sql — ONE-TIME full build of leader_state_daily (Early/Mid/TML).
-- Sibling of leader_tier_backfill.sql. Re-run after a theme-map rebuild (tiers/etf change).
-- Reproduces the stock-picks SPY RS-line (bs_ratio_over_ma) per-date; reuses bt_leader_daily
-- (uni_*_pct ✓-triple, rs_rsp, ext), v_pct_from_low, v_rmv, macro_regime_daily(SPY).
-- Also repoints v_leader_state and builds v_leader_state_current (live tab feed).
-- Run in BigQuery Studio (paste all, Run). Full history (states resolve ~1993+ where SPY exists).
-- ============================================================================
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.leader_state_daily`
PARTITION BY DATE_TRUNC(date, MONTH) CLUSTER BY ticker AS
WITH spy AS (
  SELECT DATE(date) AS date, adj_close AS spy_close
  FROM `stonks-498420.stonks_data.price_history` WHERE ticker='SPY'
),
rl0 AS (
  SELECT p.ticker, DATE(p.date) AS date, p.adj_close,
    p.adj_close/s.spy_close                AS rs_line,
    AVG(p.adj_close/s.spy_close) OVER w21   AS ma21,
    AVG(p.adj_close/s.spy_close) OVER w21p  AS ma21_prior,
    AVG(p.adj_close)             OVER w50   AS sma50
  FROM `stonks-498420.stonks_data.price_history` p
  JOIN spy s ON s.date = DATE(p.date)
  WINDOW
    w21  AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
    w21p AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING),
    w50  AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
),
rl AS (
  SELECT ticker, date, adj_close, sma50,
    (rs_line > ma21)    AS rs_line_break,   -- = stock-picks bs_ratio_over_ma
    (ma21 > ma21_prior) AS rs_ma_rising,    -- = bt_ma_rising
    (adj_close > sma50) AS above_50ma,
    (sma50 > LAG(sma50,10) OVER (PARTITION BY ticker ORDER BY date)) AS sma50_rising   -- PARAM rising lookback = 10
  FROM rl0
),
base AS (
  SELECT bt.ticker, bt.date,
    bt.uni_1m_pct, bt.uni_3m_pct, bt.uni_6m_pct, bt.rs_rsp, bt.ext,
    (bt.uni_1m_pct>=80) AS chk_1m, (bt.uni_3m_pct>=80) AS chk_3m, (bt.uni_6m_pct>=80) AS chk_6m,   -- PARAM P=80
    rl.rs_line_break, rl.rs_ma_rising, rl.above_50ma, rl.sma50_rising,
    pfl.pct_from_low_3m, pfl.pct_from_low_6m, pfl.pct_from_low_12m,
    rmv.rmv, rmv.rmv_tight, mr.risk_on,
    CASE
      WHEN pfl.pct_from_low_6m >= 1.00 AND rl.above_50ma AND bt.rs_rsp >= 0.80 THEN 'TML'          -- PARAM
      WHEN bt.uni_6m_pct>=80 AND bt.uni_3m_pct>=80 AND rl.above_50ma AND rl.sma50_rising
           AND pfl.pct_from_low_6m BETWEEN 0.40 AND 0.90 AND bt.ext < 7 THEN 'MID'                 -- PARAM
      WHEN rl.rs_line_break AND bt.uni_1m_pct>=80 AND bt.uni_6m_pct<80 AND bt.ext < 3
           AND pfl.pct_from_low_6m BETWEEN 0.15 AND 0.40 AND COALESCE(mr.risk_on,FALSE) THEN 'EARLY' -- PARAM
      ELSE NULL
    END AS state
  FROM `stonks-498420.stonks_data.bt_leader_daily` bt
  LEFT JOIN rl  ON rl.ticker=bt.ticker AND rl.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.v_pct_from_low` pfl ON pfl.ticker=bt.ticker AND pfl.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.v_rmv` rmv ON rmv.ticker=bt.ticker AND rmv.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.macro_regime_daily` mr ON mr.etf='SPY' AND mr.date=bt.date
),
mark AS (
  SELECT *, LAG(state) OVER w AS prev_state,
    CASE WHEN state IS DISTINCT FROM LAG(state) OVER w THEN 1 ELSE 0 END AS state_start
  FROM base WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
isl AS (
  SELECT *, SUM(state_start) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM mark
)
SELECT * EXCEPT(state_start, grp),
  MIN(date) OVER (PARTITION BY ticker, grp)                       AS state_entry_date,
  DATE_DIFF(date, MIN(date) OVER (PARTITION BY ticker, grp), DAY) AS days_in_state
FROM isl;

-- repoint the view to the table (existing refs keep working)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_state` AS
SELECT * FROM `stonks-498420.stonks_data.leader_state_daily`;

-- live Leaders-tab feed: latest row per ticker + display names
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_state_current` AS
SELECT s.*, tk.name AS company, tk.sector, tk.industry
FROM `stonks-498420.stonks_data.leader_state_daily` s
LEFT JOIN `stonks-498420.stonks_data.tickers` tk USING (ticker)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.date DESC) = 1;
