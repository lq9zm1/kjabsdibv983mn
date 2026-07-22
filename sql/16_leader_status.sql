-- ============================================================================
-- 16_leader_status — boolean leader flags + historical SPELLS for the 3 leader
-- categories, layered on bt_leader_daily (sql/15). Gives you:
--   • v_leader_status_daily — point-in-time TRUE/FALSE per (ticker,date) → the BACKTEST join source
--   • leader_spells         — one row per continuous leader run (became → fell out) → tab + list export
--   • v_leader_current      — latest-day snapshot of who is leading now
--
-- THE 3 KNOBS — a ticker "is" a leader in a category when (tune these numbers):
--   universe_leader : universe_leader >= 80   (top 20% of the whole universe by composite momentum)
--   theme_leader    : theme_leader_rank = 1    (the single strongest name in its sub-theme that day)
--   etf_leader      : etf_leader > 0           (outperforming its own parent-ETF's 1-month return)
--
-- Runs nightly AFTER 15_leader_engine (filename sorts after it). Idempotent + nightly-safe.
-- ============================================================================

-- 1) DAILY point-in-time flags (VIEW). Join your backtest fires to this on (ticker,date) to know,
--    as of any date, whether the name was a universe / theme / etf leader. Booleans never NULL.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_status_daily` AS
SELECT
  ticker, date, sub_theme,
  group_score, ext, rs_rsp,
  universe_leader, theme_leader_pct, theme_leader_rank, etf_leader,
  COALESCE(universe_leader >= 80, FALSE) AS is_universe_leader,     -- KNOB 1
  COALESCE(theme_leader_rank = 1,  FALSE) AS is_theme_leader,        -- KNOB 2
  COALESCE(etf_leader > 0,         FALSE) AS is_etf_leader           -- KNOB 3
FROM `stonks-498420.stonks_data.bt_leader_daily`;

-- 2) SPELLS (TABLE, rebuilt nightly). One row per continuous run of leader days per ticker per
--    category: spell_start = the day it BECAME a leader, spell_end = last leader day, is_current =
--    still leading as of the latest date. Islands are on consecutive TRADING days (weekend/holiday
--    gaps don't split a run) via the (all-day rank − leader-day rank) trick.
CREATE OR REPLACE TABLE `stonks-498420.stonks_data.leader_spells`
CLUSTER BY category, ticker AS
WITH src AS (
  SELECT ticker, date, sub_theme, is_universe_leader, is_theme_leader, is_etf_leader
  FROM `stonks-498420.stonks_data.v_leader_status_daily`
  WHERE date >= DATE '2023-01-01'          -- SPELLS HISTORY FLOOR — widen for more backtest history
),
long AS (
  SELECT ticker, date, 'universe' AS category, sub_theme, is_universe_leader AS is_leader FROM src
  UNION ALL SELECT ticker, date, 'theme', sub_theme, is_theme_leader FROM src
  UNION ALL SELECT ticker, date, 'etf',   sub_theme, is_etf_leader   FROM src
),
seq AS (   -- dn = rank over ALL days; ln (below) = rank over leader-only days
  SELECT ticker, category, sub_theme, date, is_leader,
    ROW_NUMBER() OVER (PARTITION BY ticker, category ORDER BY date) AS dn
  FROM long
),
onlead AS (
  SELECT ticker, category, sub_theme, date, dn,
    ROW_NUMBER() OVER (PARTITION BY ticker, category ORDER BY date) AS ln
  FROM seq WHERE is_leader
),
latest AS ( SELECT MAX(date) AS d FROM `stonks-498420.stonks_data.bt_leader_daily` )
SELECT
  o.ticker, o.category,
  ANY_VALUE(o.sub_theme)              AS sub_theme,
  MIN(o.date)                         AS spell_start,        -- BECAME a leader
  MAX(o.date)                         AS spell_end,          -- last leader day (fell out the next session)
  COUNT(*)                            AS n_leader_days,
  DATE_DIFF(MAX(o.date), MIN(o.date), DAY) + 1 AS span_days,
  MAX(o.date) = ANY_VALUE(l.d)        AS is_current          -- still leading as of the latest date
FROM onlead o CROSS JOIN latest l
GROUP BY o.ticker, o.category, (o.dn - o.ln);

-- 3) CURRENT snapshot (VIEW) — who is leading right now, per category. Feeds the Leaders tab's
--    "leading now" blocks and the quick current-list export.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_current` AS
SELECT ticker, sub_theme,
  universe_leader, theme_leader_pct, theme_leader_rank, etf_leader, rs_rsp, ext,
  is_universe_leader, is_theme_leader, is_etf_leader
FROM `stonks-498420.stonks_data.v_leader_status_daily`
WHERE date = (SELECT MAX(date) FROM `stonks-498420.stonks_data.bt_leader_daily`);
