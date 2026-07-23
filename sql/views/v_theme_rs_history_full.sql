-- sql/views/v_theme_rs_history_full.sql
-- Full-history theme RS = the 1993 deep backfill for old dates + the fresh nightly table for recent.
-- Run ONCE after theme_rs_history_deep_backfill.sql (BigQuery forbids creating a view in the same
-- script that defines temp UDFs, so it's split out here). Idempotent CREATE OR REPLACE — a table
-- rebuild does NOT require re-running this. Backtests should read v_theme_rs_history_full.
-- Both source tables share identical schema (the deep mirrors sql/10_theme_rs_history exactly).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_theme_rs_history_full` AS
SELECT * FROM `stonks-498420.stonks_data.theme_rs_history_deep`
WHERE date < (SELECT MIN(date) FROM `stonks-498420.stonks_data.theme_rs_history`)   -- deep = older than nightly's floor
UNION ALL
SELECT * FROM `stonks-498420.stonks_data.theme_rs_history`;                          -- nightly = recent, stays fresh
