-- ============================================================================
--  11_sata_score.sql  —  materialize v_sata_score into a physical table.
--
--  WHY: v_sata_score fans out across 10 band views over full history
--  (price_history 2000->now, ~3,000 tickers). Querying the live view re-runs
--  all 10 bands + JS UDFs every time — expensive on the free tier once the
--  dashboard / stage engine read it repeatedly. Materializing once per night
--  turns every downstream read into a cheap clustered-table scan.
--
--  Runs in run_nightly.py step 4 (sql/*.sql glob), in filename order, right
--  after 10_theme_rs_history.sql. CREATE OR REPLACE TABLE = idempotent.
--
--  Clustered by ticker (every downstream filter is WHERE ticker = ...).
--  NOT partitioned: wk is weekly (~1,350 wks since 2000) and queries filter
--  by ticker, not date-range — clustering alone is the right access pattern.
-- ============================================================================

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.sata_score`
CLUSTER BY ticker AS
SELECT * FROM `stonks-498420.stonks_data.v_sata_score`;
