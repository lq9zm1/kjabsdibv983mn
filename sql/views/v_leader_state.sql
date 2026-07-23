-- sql/views/v_leader_state.sql — thin view over the materialized leader_state_daily table.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_leader_state` AS
SELECT * FROM `stonks-498420.stonks_data.leader_state_daily`;
