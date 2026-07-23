-- sql/views/v_leader_env.sql — daily macro leader-environment (findings §5): birth rate, breadth,
-- State-1/2/3 counts + weekly Δ. Panel reads the latest row. Cheap (aggregates over leader_state_daily).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_env` AS
WITH daily AS (
  SELECT date,
    COUNT(*)                    AS n_universe,
    COUNTIF(state='EARLY')      AS n_early,
    COUNTIF(state='MID')        AS n_mid,
    COUNTIF(state='TML')        AS n_tml,
    COUNTIF(state IS NOT NULL)  AS n_leaders,
    COUNTIF(state='EARLY' AND date=state_entry_date)     AS new_early,       -- birth rate (fresh emerging)
    COUNTIF(state IS NOT NULL AND date=state_entry_date) AS new_any_leader,
    ROUND(100*AVG(CAST(above_50ma AS INT64)),1)          AS pct_above_50ma,  -- absolute breadth (time-varying)
    ROUND(100*COUNTIF(state IS NOT NULL)/COUNT(*),2)     AS pct_in_leader_state
  FROM `stonks-498420.stonks_data.leader_state_daily`
  GROUP BY date
)
SELECT *,
  n_early - LAG(n_early,5) OVER w                        AS d_early_1w,   -- PARAM 1w = 5 trading days
  n_mid   - LAG(n_mid,5)   OVER w                        AS d_mid_1w,
  n_tml   - LAG(n_tml,5)   OVER w                        AS d_tml_1w,
  ROUND(pct_above_50ma - LAG(pct_above_50ma,5) OVER w,1) AS d_breadth_1w
FROM daily
WINDOW w AS (ORDER BY date);
