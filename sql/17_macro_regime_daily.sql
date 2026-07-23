-- sql/17_macro_regime_daily.sql — NIGHTLY full rebuild (idempotent CREATE OR REPLACE).
-- Runs in run_nightly.py step 4 (glob sql/*.sql, filename order). Reads etf_prices + etf_universe
-- + persistent UDFs (sata_macd, rt_ema). Cheap (window fns + array UDFs over ~145 ETFs). Sorts
-- after 16_* so it runs late; no downstream nightly file depends on it.
--
-- ============================================================================
-- macro_regime_daily — HISTORICAL "Layer A" macro / index-proxy regime, per (index, day) to inception.
--
-- WHAT: byte-faithful generalization of the LIVE view `v_index_proxy_ma` from
--       LATEST-ONLY to EVERY trading day, for the 6 benchmark ETFs, over their full
--       `etf_prices` history (SPY→1993, QQQ→1999, IWM/IWO→2000, RSP→2003, QQQE→2012).
--       This is the price-regime layer ONLY (Layer A): NO holdings, NO breadth, NO Index Status
--       (Index Status stays live-only — it depends on per-ETF holdings we can't reconstruct historically).
--
-- COLUMNS: tideline (Risk On/Off) + risk_on BOOL, market_trend, ma_status (6-tier ladder),
--          abv_5/10/20/50/200, atr_ext ("1 Stop Slop"), plus adj_close/close/SMAs so you can
--          compute forward returns straight off this table (backtest "QQQ+SPY vs all indexes").
--
-- FAITHFULNESS NOTES (so verification lines up, not a bug):
--   • SMAs 5/10/20/50/200 = same window definitions as v_index_proxy_ma (ROWS BETWEEN N PRECEDING).
--   • MACD(6,20,9) via the SAME persistent UDF `sata_macd`, run once over each ETF's full close
--     history, then expanded to one row per bar with UNNEST … WITH OFFSET.
--   • TIDELINE keeps the live view's exact 1-BAR LAG: it uses the PREVIOUS bar's MACD sign
--     (`arr[n-2]` in the live view → `mp.k = s.k - 1` here). So the LATEST row of this table
--     EQUALS today's live dashboard. (To use same-bar MACD instead, change `s.k - 1` → `s.k`.)
--   • ATR = the SAME persistent UDF `rt_ema(trs,27)` over the true-range series (first bar per
--     ticker drops out because TR needs a prior close), aligned back to its bar.
--   • atr_ext uses RAW close vs sma50_raw, exactly as the live view.
--
-- COST/REFRESH: cheap enough to CREATE OR REPLACE in full nightly (no incremental upsert needed →
--   cannot go stale the way SATA did). Wire into the nightly executor as a full rebuild when ready.
--
-- ★ v2 (2026-07-23): now covers EVERY ETF in `etf_prices` (macro + the 11 SPDR sectors + thematic
--   ETFs), with a `category` column from `etf_universe` ('macro'/'sector'/'etf'/NULL). The 6 macro
--   ETFs keep identical values (the faithfulness check still passes for them); sectors/themes are
--   additive so sector & parent-ETF risk-on/off backtests work. Filter `category='macro'` for the
--   original 6, `category='sector'` for SPDRs, or by explicit ticker.
-- ★ v3 (2026-07-23): substage is NOT here — it's the BIT-EXACT SATA stage engine run on ETF prices,
--   in its own isolated table `stage_engine_v2_etf` (weekly), built by `etf_stage_engine_build.sql`.
--   This daily table stays the pure price-regime layer; join the weekly substage via v_theme_parent_regime.
--
-- CAVEATS: first ~200 bars per ETF have partial SMAs (window still filling) and the first ~1-2
--   months of MACD are seed-unstable — i.e. the earliest weeks of each ETF's history are warmup.
-- ============================================================================

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.macro_regime_daily`
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY etf AS
WITH
-- One clean row per (ticker, date); dedupe guards the array/row-number offset alignment below.
px AS (
  SELECT p.ticker, DATE(p.date) AS date, p.adj_close, p.close, p.high, p.low, u.category
  FROM `stonks-498420.stonks_data.etf_prices` p
  LEFT JOIN `stonks-498420.stonks_data.etf_universe` u USING (ticker)   -- category: macro/sector/etf
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.ticker, DATE(p.date) ORDER BY p.date DESC) = 1
),
-- Moving averages (identical windows to v_index_proxy_ma) + a 0-based per-ticker bar offset `k`.
sm AS (
  SELECT ticker, date, category, adj_close, close, high, low,
    AVG(adj_close) OVER w5   AS sma5,
    AVG(adj_close) OVER w10  AS sma10,
    AVG(adj_close) OVER w20  AS sma20,
    AVG(adj_close) OVER w50  AS sma50,
    AVG(adj_close) OVER w200 AS sma200,
    AVG(close)     OVER w50  AS sma50_raw,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) - 1 AS k
  FROM px
  WINDOW
    w5   AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4   PRECEDING AND CURRENT ROW),
    w10  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9   PRECEDING AND CURRENT ROW),
    w20  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19  PRECEDING AND CURRENT ROW),
    w50  AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49  PRECEDING AND CURRENT ROW),
    w200 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
),
sm2 AS (
  SELECT *, LAG(sma20) OVER (PARTITION BY ticker ORDER BY date) AS sma20_prev FROM sm
),
-- MACD(6,20,9) over each ETF's FULL adj_close history (same UDF as the live view), expanded per bar.
tide AS (
  SELECT ticker, ARRAY_AGG(adj_close ORDER BY date) AS closes
  FROM px GROUP BY ticker
),
tcalc AS (
  SELECT ticker, `stonks-498420.stonks_data.sata_macd`(closes, 6, 20, 9) AS arr FROM tide
),
macd_by_bar AS (
  SELECT ticker, k, m.macd AS macd
  FROM tcalc, UNNEST(arr) AS m WITH OFFSET AS k
),
-- ATR: rt_ema(trs,27) over the true-range series (first bar per ticker has NULL TR → dropped),
-- so trs offset j maps back to all-bar offset (j+1).
tr AS (
  SELECT ticker, date,
    GREATEST(high - low,
             ABS(high - LAG(close) OVER (PARTITION BY ticker ORDER BY date)),
             ABS(low  - LAG(close) OVER (PARTITION BY ticker ORDER BY date))) AS trv
  FROM px
),
tragg AS (
  SELECT ticker, ARRAY_AGG(trv ORDER BY date) AS trs
  FROM tr WHERE trv IS NOT NULL GROUP BY ticker
),
atrcalc AS (
  SELECT ticker, `stonks-498420.stonks_data.rt_ema`(trs, 27) AS aa FROM tragg
),
atr_by_bar AS (
  SELECT ticker, j + 1 AS k, a AS atrv
  FROM atrcalc, UNNEST(aa) AS a WITH OFFSET AS j
)
SELECT
  s.ticker AS etf,
  s.category,
  s.date,
  s.adj_close, s.close,
  s.sma5, s.sma10, s.sma20, s.sma50, s.sma200,
  IF(s.adj_close > s.sma5,   'Above','Below') AS abv_5,
  IF(s.adj_close > s.sma10,  'Above','Below') AS abv_10,
  IF(s.adj_close > s.sma20,  'Above','Below') AS abv_20,
  IF(s.adj_close > s.sma50,  'Above','Below') AS abv_50,
  IF(s.adj_close > s.sma200, 'Above','Below') AS abv_200,
  CASE
    WHEN s.adj_close <= s.sma200 THEN 'Downtrend'
    WHEN s.adj_close <= s.sma50  THEN 'Avoid'
    WHEN s.adj_close <= s.sma20  THEN 'Pullback'
    WHEN s.adj_close <= s.sma10  THEN 'Uptrend'
    WHEN s.adj_close <= s.sma5   THEN 'Strong'
    ELSE 'Exceptional'
  END AS ma_status,
  mp.macd AS tide_macd,                                   -- previous-bar MACD value (magnitude)
  CASE WHEN mp.macd IS NULL THEN '' WHEN mp.macd > 0 THEN 'Risk On' ELSE 'Risk Off' END AS tideline,
  CASE WHEN mp.macd IS NULL THEN NULL ELSE mp.macd > 0 END AS risk_on,
  CASE
    WHEN s.adj_close >  s.sma20 AND s.sma20 >  s.sma20_prev THEN 'Above Rising'
    WHEN s.adj_close >  s.sma20 AND s.sma20 <= s.sma20_prev THEN 'Above Declining'
    WHEN s.adj_close <= s.sma20 AND s.sma20 >  s.sma20_prev THEN 'Below Rising'
    ELSE 'Below Declining'
  END AS market_trend,
  ROUND(SAFE_DIVIDE((SAFE_DIVIDE(s.close, s.sma50_raw) - 1) * s.close, ab.atrv), 2) AS atr_ext
FROM sm2 s
LEFT JOIN macd_by_bar mp ON mp.ticker = s.ticker AND mp.k = s.k - 1   -- 1-bar tideline lag (matches live view)
LEFT JOIN atr_by_bar  ab ON ab.ticker = s.ticker AND ab.k = s.k;      -- this bar's ATR
