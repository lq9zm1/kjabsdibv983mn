-- ============================================================================
-- sql/18_leader_state_nightly.sql
-- LIGHT nightly refresh for leader_state_daily (sibling of sql/16_leader_status.sql).
-- run_nightly.py globs sql/*.sql in filename order, so "18" runs after "17_macro_regime_daily".
-- Recomputes the recent tail of leader_state_daily + re-islands across the boundary
-- (carries each ticker's real state_entry_date) and swaps the tail. Full history untouched.
-- Depends on live BQ objects: bt_leader_daily, v_pct_from_low, v_rmv, macro_regime_daily, price_history.
-- ============================================================================
DECLARE recent_floor DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);    -- PARAM refresh window
DECLARE calc_floor   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 140 DAY);   -- PARAM window warmup (>=50d MAs)

CREATE TEMP TABLE recompute AS
WITH spy AS (
  SELECT DATE(date) AS date, adj_close AS spy_close
  FROM `stonks-498420.stonks_data.price_history` WHERE ticker='SPY' AND DATE(date) >= calc_floor
),
rl0 AS (
  SELECT p.ticker, DATE(p.date) AS date, p.adj_close,
    p.adj_close/s.spy_close AS rs_line,
    AVG(p.adj_close/s.spy_close) OVER w21 AS ma21, AVG(p.adj_close/s.spy_close) OVER w21p AS ma21_prior,
    AVG(p.adj_close) OVER w50 AS sma50
  FROM `stonks-498420.stonks_data.price_history` p JOIN spy s ON s.date=DATE(p.date)
  WHERE DATE(p.date) >= calc_floor
  WINDOW w21  AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
         w21p AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING),
         w50  AS (PARTITION BY p.ticker ORDER BY DATE(p.date) ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
),
rl AS (
  SELECT ticker, date, adj_close, sma50, (rs_line>ma21) AS rs_line_break, (ma21>ma21_prior) AS rs_ma_rising,
    (adj_close>sma50) AS above_50ma,
    (sma50 > LAG(sma50,10) OVER (PARTITION BY ticker ORDER BY date)) AS sma50_rising
  FROM rl0
),
base AS (
  SELECT bt.ticker, bt.date, bt.uni_1m_pct, bt.uni_3m_pct, bt.uni_6m_pct, bt.rs_rsp, bt.ext,
    (bt.uni_1m_pct>=80) AS chk_1m, (bt.uni_3m_pct>=80) AS chk_3m, (bt.uni_6m_pct>=80) AS chk_6m,
    rl.rs_line_break, rl.rs_ma_rising, rl.above_50ma, rl.sma50_rising,
    pfl.pct_from_low_3m, pfl.pct_from_low_6m, pfl.pct_from_low_12m, rmv.rmv, rmv.rmv_tight, mr.risk_on,
    CASE
      WHEN pfl.pct_from_low_6m>=1.00 AND rl.above_50ma AND bt.rs_rsp>=0.80 THEN 'TML'
      WHEN bt.uni_6m_pct>=80 AND bt.uni_3m_pct>=80 AND rl.above_50ma AND rl.sma50_rising
           AND pfl.pct_from_low_6m BETWEEN 0.40 AND 0.90 AND bt.ext<7 THEN 'MID'
      WHEN rl.rs_line_break AND bt.uni_1m_pct>=80 AND bt.uni_6m_pct<80 AND bt.ext<3
           AND pfl.pct_from_low_6m BETWEEN 0.15 AND 0.40 AND COALESCE(mr.risk_on,FALSE) THEN 'EARLY'
      ELSE NULL END AS state
  FROM `stonks-498420.stonks_data.bt_leader_daily` bt
  LEFT JOIN rl ON rl.ticker=bt.ticker AND rl.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.v_pct_from_low` pfl ON pfl.ticker=bt.ticker AND pfl.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.v_rmv` rmv ON rmv.ticker=bt.ticker AND rmv.date=bt.date
  LEFT JOIN `stonks-498420.stonks_data.macro_regime_daily` mr ON mr.etf='SPY' AND mr.date=bt.date
  WHERE bt.date >= calc_floor
),
seed AS (   -- each ticker's last row BEFORE the window, carrying its REAL state_entry_date
  SELECT ticker, date, state, state_entry_date, TRUE AS is_seed
  FROM `stonks-498420.stonks_data.leader_state_daily`
  WHERE date < recent_floor AND date >= DATE_SUB(recent_floor, INTERVAL 20 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC)=1
),
seq AS (
  SELECT ticker, date, state, CAST(NULL AS DATE) AS seed_entry, FALSE AS is_seed FROM base WHERE date >= recent_floor
  UNION ALL SELECT ticker, date, state, state_entry_date, TRUE FROM seed
),
mark AS (   -- LAG here (not inside the SUM below) → no nested analytics
  SELECT ticker, date, is_seed, seed_entry,
    CASE WHEN state IS DISTINCT FROM LAG(state) OVER w THEN 1 ELSE 0 END AS state_start,
    LAG(state) OVER w AS prev_state
  FROM seq WINDOW w AS (PARTITION BY ticker ORDER BY date)
),
isl AS (
  SELECT ticker, date, is_seed, seed_entry, prev_state,
    SUM(state_start) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM mark
),
entry AS (
  SELECT ticker, date, is_seed, prev_state,
    COALESCE(MAX(IF(is_seed, seed_entry, NULL)) OVER (PARTITION BY ticker, grp),
             MIN(date) OVER (PARTITION BY ticker, grp)) AS state_entry_date
  FROM isl
)
SELECT b.*, e.prev_state, e.state_entry_date, DATE_DIFF(b.date, e.state_entry_date, DAY) AS days_in_state
FROM base b JOIN entry e ON e.ticker=b.ticker AND e.date=b.date AND e.is_seed=FALSE
WHERE b.date >= recent_floor;

DELETE FROM `stonks-498420.stonks_data.leader_state_daily` WHERE date >= recent_floor;
INSERT INTO `stonks-498420.stonks_data.leader_state_daily` SELECT * FROM recompute;
