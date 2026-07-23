-- sql/views/v_leader_state_current.sql — latest row per ticker + display names (Leaders-tab feed).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_state_current` AS
SELECT s.*, tk.name AS company, tk.sector, tk.industry
FROM `stonks-498420.stonks_data.leader_state_daily` s
LEFT JOIN `stonks-498420.stonks_data.tickers` tk USING (ticker)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.date DESC) = 1;
