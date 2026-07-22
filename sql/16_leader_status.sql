-- ============================================================================
-- 16_leader_status — historical leader signals for backtesting. Two families:
--   (A) UNIVERSE + ETF leader flags — binary, from bt_leader_daily (sql/15), matching the Macro sheet.
--   (B) THEME LEADER / MID / LAGGARD tier — the adaptive group_score BAND, identical to your Theme View
--       (v_stock_dashboard.theme_rank), with the CORRECTED group_score = AVG of the 5 returns (NO atr_pct).
--   Plus (C) the full tier STATE-CHANGE history (Leader→Mid→Laggard→Mid→Leader …) for transition backtests
--   and (D) a current snapshot with days-since-last-Laggard / -Leader.
--
-- ⚠ BACKEND SYNC (do right after deploying this): v_stock_dashboard's `d` CTE currently computes
--   group_score = (SUM of 5 returns)/atr_pct. Fix it to the SAME AVG form used here so the live Theme
--   View and this historical tier are identical:
--     ROUND((SELECT AVG(x) FROM UNNEST([m.ret_1d,m.ret_1w,m.ret_1m,m.ret_3m,m.ret_6m]) x),4) AS group_score
--
-- Grain: (A) per (ticker,date) full history; (B)-(D) per (ticker,theme,date), history = metrics_daily (2024-01-01+).
-- Runs nightly AFTER 15_leader_engine. Idempotent + nightly-safe.
-- ============================================================================

-- ---- (A) UNIVERSE + ETF binary flags (Macro-matching) — the point-in-time backtest join source -----
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_status_daily` AS
SELECT ticker, date, sub_theme, group_score, ext, rs_rsp, universe_leader, etf_leader,
  COALESCE(universe_leader >= 80, FALSE) AS is_universe_leader,   -- top 20% of the universe (Macro n_strong_80)
  COALESCE(etf_leader > 0,        FALSE) AS is_etf_leader          -- beating its own ETF's 1M (Macro n_beating_own_etf)
FROM `stonks-498420.stonks_data.bt_leader_daily`;

-- ---- (B) THEME tier: adaptive group_score band = your Theme View / v_stock_dashboard.theme_rank -----
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.leader_tier_daily`
PARTITION BY DATE_TRUNC(date, MONTH) CLUSTER BY theme, ticker AS
WITH gs AS (   -- per (ticker, theme, date): group_score = AVG of the 5 returns (NO atr_pct). Sourced from
               -- bt_leader_daily (sql/15), which already computes that average HISTORICALLY over full
               -- price_history — metrics_daily is a LATEST-ONLY snapshot, so it can't feed the history.
  SELECT l.ticker, l.date, s.sub_theme AS theme, s.main_theme, s.etf,
    l.group_score,        -- = AVG(ret_1d,1w,1m,3m,6m) — the corrected form (matches theme_stats + fixed v_stock_dashboard)
    l.rs_rsp              -- historical leader-hunter RS (Mansfield rs_value_21d is latest-only, not kept here)
  FROM `stonks-498420.stonks_data.bt_leader_daily` l
  JOIN `stonks-498420.stonks_data.stock_theme_map` s
    ON REGEXP_REPLACE(UPPER(s.ticker), r'[./]', '-') = l.ticker   -- normalize dot/slash share-class to price_history's dash form
  WHERE s.sub_theme IS NOT NULL
    -- FULL history: no date floor (bt_leader_daily + fires both start 1962). Add `AND l.date >= DATE 'YYYY-01-01'` to cap it.
),
bands AS (   -- per (theme, date): mean + spread of member group_scores (theme_stats logic, sql/03)
  SELECT theme, date, AVG(group_score) AS avg_gs, STDDEV_SAMP(group_score) AS spread
  FROM gs GROUP BY theme, date
),
ranges AS (   -- adaptive band: avg ± spread × volatility-scaled multiplier
  SELECT theme, date, avg_gs, spread,
    avg_gs + spread * mult AS upper_range,
    avg_gs - spread * mult AS lower_range
  FROM (
    SELECT theme, date, avg_gs, spread,
      CASE WHEN SAFE_DIVIDE(spread, avg_gs) < 0.3 THEN 0.4
           WHEN SAFE_DIVIDE(spread, avg_gs) < 0.6 THEN 0.6
           WHEN SAFE_DIVIDE(spread, avg_gs) < 1   THEN 0.8 ELSE 1 END AS mult
    FROM bands
  )
)
SELECT g.ticker, g.date, g.theme, g.main_theme, g.etf,
  ROUND(g.group_score, 4) AS group_score, g.rs_rsp,
  ROUND(r.upper_range, 4) AS upper_range, ROUND(r.lower_range, 4) AS lower_range,
  CASE WHEN g.group_score IS NULL OR g.theme IS NULL THEN NULL
       WHEN g.group_score > r.upper_range THEN 'Leader'
       WHEN g.group_score < r.lower_range THEN 'Laggard'
       ELSE 'Mid' END AS leader_tier
FROM gs g JOIN ranges r USING (theme, date);

-- ---- (C) TIER STATE-CHANGE spells: one row per continuous tier run per (ticker, theme) -------------
--      Ordered by spell_start it reads as the full path: Leader→Mid→Laggard→Mid→Leader …
--      prev_tier / next_tier make each transition explicit (prev='Laggard', tier='Mid' = climbed out;
--      prev='Leader', tier='Mid' = slipped). Islands are on tier-VALUE change (weekend gaps don't split).
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.leader_tier_spells`
CLUSTER BY ticker, theme AS
WITH seq AS (
  SELECT ticker, theme, date, leader_tier,
    LAG(leader_tier) OVER (PARTITION BY ticker, theme ORDER BY date) AS prev_day_tier
  FROM `stonks-498420.stonks_data.leader_tier_daily`
  WHERE leader_tier IS NOT NULL
),
marked AS (   -- running count of tier changes = a stable id per continuous tier run
  SELECT ticker, theme, date, leader_tier,
    COUNTIF(prev_day_tier IS NULL OR leader_tier != prev_day_tier)
      OVER (PARTITION BY ticker, theme ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS spell_id
  FROM seq
),
spells AS (
  SELECT ticker, theme, ANY_VALUE(leader_tier) AS tier,
    MIN(date) AS spell_start, MAX(date) AS spell_end, COUNT(*) AS n_days,
    MAX(date) = (SELECT MAX(date) FROM `stonks-498420.stonks_data.leader_tier_daily`) AS is_current
  FROM marked
  GROUP BY ticker, theme, spell_id
)
SELECT ticker, theme, tier, spell_start, spell_end, n_days, is_current,
  LAG(tier)  OVER (PARTITION BY ticker, theme ORDER BY spell_start) AS prev_tier,   -- what it came FROM
  LEAD(tier) OVER (PARTITION BY ticker, theme ORDER BY spell_start) AS next_tier    -- what it went TO
FROM spells;

-- ---- (D) CURRENT snapshot per (ticker, theme): tier now + recency of last Laggard / Leader ---------
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_tier_current` AS
WITH latest AS (SELECT MAX(date) AS d FROM `stonks-498420.stonks_data.leader_tier_daily`),
cur AS (
  SELECT t.ticker, t.theme, t.main_theme, t.etf, t.group_score, t.rs_rsp, t.leader_tier AS current_tier
  FROM `stonks-498420.stonks_data.leader_tier_daily` t, latest
  WHERE t.date = latest.d
),
recency AS (   -- last day each (ticker,theme) sat in each tier — these ARE the dates you can match on
  SELECT ticker, theme,
    MAX(IF(leader_tier = 'Leader',  date, NULL)) AS last_leader_date,
    MAX(IF(leader_tier = 'Mid',     date, NULL)) AS last_mid_date,
    MAX(IF(leader_tier = 'Laggard', date, NULL)) AS last_laggard_date
  FROM `stonks-498420.stonks_data.leader_tier_daily`
  GROUP BY ticker, theme
)
SELECT c.ticker, c.theme, c.main_theme, c.etf, c.group_score, c.rs_rsp, c.current_tier,
  rc.last_leader_date, rc.last_mid_date, rc.last_laggard_date,
  DATE_DIFF((SELECT d FROM latest), rc.last_leader_date,  DAY) AS days_since_leader,
  DATE_DIFF((SELECT d FROM latest), rc.last_mid_date,     DAY) AS days_since_mid,
  DATE_DIFF((SELECT d FROM latest), rc.last_laggard_date, DAY) AS days_since_laggard    -- just-climbed-out signal
FROM cur c LEFT JOIN recency rc USING (ticker, theme);

-- ---- (E) OPTION-B join source: ONE row per (ticker, date) — the tier in the stock's strongest theme.
--      Join your fires on (ticker, date) with no theme needed. Full 2024+ history (self-contained; picks
--      the theme where the name's own group_score is highest = its leading theme).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_tier_primary` AS
SELECT * EXCEPT(rn) FROM (
  SELECT td.*,
    ROW_NUMBER() OVER (PARTITION BY td.ticker, td.date ORDER BY td.group_score DESC NULLS LAST) AS rn
  FROM `stonks-498420.stonks_data.leader_tier_daily` td
)
WHERE rn = 1;
