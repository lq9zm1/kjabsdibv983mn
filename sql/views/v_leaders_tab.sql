-- sql/views/v_leaders_tab.sql — live Leaders-tab feed (latest per ticker): "On" columns + State +
-- state-transition + leader/mid/laggard tier & tier-change + parent-theme ETF comparables.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leaders_tab` AS
WITH ltd_latest AS (SELECT MAX(date) AS d FROM `stonks-498420.stonks_data.leader_tier_daily`),
prim AS (   -- each ticker's PRIMARY theme (strongest group_score) on the latest date + its tier
  SELECT ticker, theme AS primary_theme, etf AS theme_etf, leader_tier AS tier, group_score
  FROM `stonks-498420.stonks_data.leader_tier_daily`
  WHERE date = (SELECT d FROM ltd_latest)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY group_score DESC) = 1
),
prev AS (   -- that primary theme's tier at its immediately-prior date -> tier-change
  SELECT l.ticker, l.leader_tier AS prev_tier
  FROM `stonks-498420.stonks_data.leader_tier_daily` l
  JOIN prim p ON p.ticker=l.ticker AND p.primary_theme=l.theme
  WHERE l.date < (SELECT d FROM ltd_latest)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY l.ticker ORDER BY l.date DESC) = 1
),
comp AS (   -- parent-theme ETF comparable for the primary theme (latest)
  SELECT v.ticker, v.parent_etf, v.petf_substage, v.petf_broad_stage, v.petf_risk_on,
    CASE WHEN v.petf_substage IN ('2A','2','2-','2−')  THEN 'Fresh-Mid'
         WHEN v.petf_substage IN ('2B','2B-','2B−')    THEN 'Late-Adv'
         ELSE v.petf_substage END AS parent_stage_class
  FROM `stonks-498420.stonks_data.v_theme_parent_regime` v
  JOIN prim p ON p.ticker=v.ticker AND p.primary_theme=v.theme
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ticker ORDER BY v.date DESC) = 1
)
SELECT
  s.ticker, s.company, s.sector, s.industry,
  ROUND(100*m.ret_6m,1) AS pct_6m, ROUND(100*m.ret_3m,1) AS pct_3m, ROUND(100*m.ret_1m,1) AS pct_1m,
  IF(s.chk_6m,'✓','') AS f6m, IF(s.chk_3m,'✓','') AS f3m, IF(s.chk_1m,'✓','') AS f1m,
  ROUND(s.rs_rsp,3) AS rs_rsp, s.rs_line_break AS rs_break, ROUND(s.ext,2) AS ext,
  m.avg_dollar_vol, ROUND(s.rmv,1) AS rmv, s.rmv_tight,
  s.state, s.state_entry_date, s.days_in_state,
  CASE WHEN s.date = s.state_entry_date
       THEN COALESCE(s.prev_state,'ø')||'→'||COALESCE(s.state,'ø') END AS state_transition_today,
  p.primary_theme, p.tier AS leader_tier,
  CASE WHEN p.tier IS DISTINCT FROM pv.prev_tier
       THEN COALESCE(pv.prev_tier,'—')||'→'||p.tier END AS tier_change,          -- newly-leader / fell-to-laggard
  c.parent_etf, c.petf_substage AS parent_substage, c.parent_stage_class,
  c.petf_broad_stage AS parent_broad_stage, c.petf_risk_on AS parent_risk_on,
  ROUND(100*s.pct_from_low_6m,1) AS pct_from_low_6m, s.above_50ma, s.sma50_rising, s.risk_on
FROM `stonks-498420.stonks_data.v_leader_state_current` s
LEFT JOIN `stonks-498420.stonks_data.metrics_daily` m USING (ticker)
LEFT JOIN prim p USING (ticker)
LEFT JOIN prev pv USING (ticker)
LEFT JOIN comp c USING (ticker);
