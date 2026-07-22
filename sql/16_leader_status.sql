-- ============================================================================
-- 16_leader_tier_nightly — NIGHTLY, LIGHT. The full 1962+ history is built ONCE by
-- leader_tier_backfill.sql (re-run only when the theme map changes). This file does NOT recompute
-- history — it only:
--   1) upserts the recent ~7 trading days into leader_tier_daily (recent-day bands only), then
--   2) re-islands leader_tier_spells from the already-computed tier column and refreshes the views.
-- The expensive part (bands over all history) never runs here. Requires leader_tier_daily to already
-- exist (from the backfill). Goes in sql/, runs AFTER 15_leader_engine.
-- ============================================================================

-- Compute the recent-window floor ONCE (a table-subquery can't live inside a MERGE ON predicate).
DECLARE recent_floor DATE DEFAULT (
  SELECT DATE_SUB(MAX(date), INTERVAL 7 DAY) FROM `stonks-498420.stonks_data.bt_leader_daily`
);

-- 1) RECENT-DAY UPSERT — recompute only the last 7 days' tier (their bands use only those days' members).
MERGE `stonks-498420.stonks_data.leader_tier_daily` T
USING (
  WITH gs AS (
    SELECT l.ticker, l.date, s.sub_theme AS theme, s.main_theme, s.etf, l.group_score, l.rs_rsp
    FROM `stonks-498420.stonks_data.bt_leader_daily` l
    JOIN `stonks-498420.stonks_data.stock_theme_map` s
      ON REGEXP_REPLACE(UPPER(s.ticker), r'[./]', '-') = l.ticker
    WHERE s.sub_theme IS NOT NULL
      AND l.date >= recent_floor
  ),
  bands AS (SELECT theme, date, AVG(group_score) AS avg_gs, STDDEV_SAMP(group_score) AS spread FROM gs GROUP BY theme, date),
  ranges AS (
    SELECT theme, date, avg_gs + spread * mult AS upper_range, avg_gs - spread * mult AS lower_range
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
  FROM gs g JOIN ranges r USING (theme, date)
) S
ON  T.ticker = S.ticker AND T.theme = S.theme AND T.date = S.date
    AND T.date >= recent_floor
WHEN MATCHED THEN UPDATE SET
  main_theme = S.main_theme, etf = S.etf, group_score = S.group_score, rs_rsp = S.rs_rsp,
  upper_range = S.upper_range, lower_range = S.lower_range, leader_tier = S.leader_tier
WHEN NOT MATCHED THEN INSERT
  (ticker, date, theme, main_theme, etf, group_score, rs_rsp, upper_range, lower_range, leader_tier)
  VALUES (S.ticker, S.date, S.theme, S.main_theme, S.etf, S.group_score, S.rs_rsp, S.upper_range, S.lower_range, S.leader_tier);

-- 2) RE-ISLAND SPELLS from the already-computed tier column (light — reads (ticker,theme,date,tier) only,
--    no band recompute) so the transition history includes the day just upserted.
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.leader_tier_spells`
CLUSTER BY ticker, theme AS
WITH seq AS (
  SELECT ticker, theme, date, leader_tier,
    LAG(leader_tier) OVER (PARTITION BY ticker, theme ORDER BY date) AS prev_day_tier
  FROM `stonks-498420.stonks_data.leader_tier_daily`
  WHERE leader_tier IS NOT NULL
),
marked AS (
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
  LAG(tier)  OVER (PARTITION BY ticker, theme ORDER BY spell_start) AS prev_tier,
  LEAD(tier) OVER (PARTITION BY ticker, theme ORDER BY spell_start) AS next_tier
FROM spells;

-- 3) VIEWS — cheap, always reflect the latest leader_tier_daily.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_status_daily` AS
SELECT ticker, date, sub_theme, group_score, ext, rs_rsp, universe_leader, etf_leader,
  COALESCE(universe_leader >= 80, FALSE) AS is_universe_leader,
  COALESCE(etf_leader > 0,        FALSE) AS is_etf_leader
FROM `stonks-498420.stonks_data.bt_leader_daily`;

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_tier_current` AS
WITH latest AS (SELECT MAX(date) AS d FROM `stonks-498420.stonks_data.leader_tier_daily`),
cur AS (
  SELECT t.ticker, t.theme, t.main_theme, t.etf, t.group_score, t.rs_rsp, t.leader_tier AS current_tier
  FROM `stonks-498420.stonks_data.leader_tier_daily` t, latest
  WHERE t.date = latest.d
),
recency AS (
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
  DATE_DIFF((SELECT d FROM latest), rc.last_laggard_date, DAY) AS days_since_laggard
FROM cur c LEFT JOIN recency rc USING (ticker, theme);

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_tier_primary` AS
SELECT * EXCEPT(rn) FROM (
  SELECT td.*,
    ROW_NUMBER() OVER (PARTITION BY td.ticker, td.date ORDER BY td.group_score DESC NULLS LAST) AS rn
  FROM `stonks-498420.stonks_data.leader_tier_daily` td
)
WHERE rn = 1;
