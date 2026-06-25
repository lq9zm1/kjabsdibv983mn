-- ============================================================================
--  SATA Score — full BigQuery rebuild (disaster-recovery / version control)
--  Project: stonks-498420   Dataset: stonks_data
--
--  Run this file top-to-bottom to recreate the ENTIRE SATA layer from scratch.
--  All statements are CREATE OR REPLACE (idempotent — safe to re-run).
--  Dependency order: UDFs -> base views -> band views -> v_sata_score.
--
--  NOTE: This pipeline is NOT part of the nightly job. The views auto-reflect
--  price_history / spx_daily on every query, so no nightly rebuild is needed for
--  correctness. This file exists only so the definitions live in source control.
--
--  Captured state: includes the 29-bar Volume warmup (band_02_volume) AND the
--  Mansfield cash-SPX fix (v6d): v_spx_weekly resamples the CASH index from
--  spx_daily, and band_05_mansfield joins SAME-WEEK SPX with NO lag.
--  Validated 100% vs the creator (ADBE/MSFT/WMT) across full history.
-- ============================================================================


-- ========================= 1. JAVASCRIPT UDFs ===============================
-- EMA seed = SMA-of-first-N (TradingView ta.ema convention).
-- length params are FLOAT64 + Number() — BQ passes INT64 to JS as STRING.

CREATE OR REPLACE FUNCTION `stonks-498420`.stonks_data.sata_ema(arr ARRAY<FLOAT64>, len FLOAT64)
RETURNS ARRAY<FLOAT64> LANGUAGE js AS r"""
  len = Number(len);
  var out = new Array(arr.length).fill(null);
  var idx = [];
  for (var i=0;i<arr.length;i++) if (arr[i] !== null) idx.push(i);
  if (idx.length < len) return out;
  var alpha = 2.0/(len+1.0);
  var s = 0.0;
  for (var k=0;k<len;k++) s += arr[idx[k]];
  var prev = s/len;
  out[idx[len-1]] = prev;
  for (var k=len;k<idx.length;k++){
    var i = idx[k];
    prev = alpha*arr[i] + (1.0-alpha)*prev;
    out[i] = prev;
  }
  return out;
""";


CREATE OR REPLACE FUNCTION `stonks-498420`.stonks_data.sata_macd(closes ARRAY<FLOAT64>, fast FLOAT64, slow FLOAT64, sig FLOAT64)
RETURNS ARRAY<STRUCT<macd FLOAT64, signal FLOAT64, hist FLOAT64>> LANGUAGE js AS r"""
  function ema(a, len){
    len = Number(len);                       // guard: force numeric
    var out = new Array(a.length).fill(null);
    var idx = [];
    for (var i=0;i<a.length;i++) if (a[i] !== null) idx.push(i);
    if (idx.length < len) return out;
    var alpha = 2.0/(len+1.0);
    var s = 0.0;
    for (var k=0;k<len;k++) s += a[idx[k]];
    var prev = s/len;                        // SMA-of-N seed
    out[idx[len-1]] = prev;
    for (var k=len;k<idx.length;k++){
      var i = idx[k];
      prev = alpha*a[i] + (1.0-alpha)*prev;
      out[i] = prev;
    }
    return out;
  }
  if (closes === null) return [];
  var ef = ema(closes, fast), es = ema(closes, slow);
  var macd = new Array(closes.length).fill(null);
  for (var i=0;i<closes.length;i++)
    if (ef[i] !== null && es[i] !== null) macd[i] = ef[i] - es[i];
  var sg = ema(macd, sig);
  var out = [];
  for (var i=0;i<closes.length;i++){
    var m = macd[i], sgi = sg[i];
    out.push({macd:m, signal:sgi, hist:(m!==null && sgi!==null) ? m - sgi : null});
  }
  return out;
""";


CREATE OR REPLACE FUNCTION `stonks-498420`.stonks_data.sata_volume(closes ARRAY<FLOAT64>, vols ARRAY<FLOAT64>, vNet FLOAT64, vPre FLOAT64, vFast FLOAT64, vSlow FLOAT64)
RETURNS ARRAY<STRUCT<fast FLOAT64, slow FLOAT64, p2 INT64>> LANGUAGE js AS r"""
  vNet=Number(vNet); vPre=Number(vPre); vFast=Number(vFast); vSlow=Number(vSlow);
  var n=closes.length;
  function ema(a,len){
    len=Number(len);
    var out=new Array(a.length).fill(null);
    var idx=[]; for(var i=0;i<a.length;i++) if(a[i]!==null) idx.push(i);
    if(idx.length<len) return out;
    var al=2.0/(len+1.0), s=0.0;
    for(var k=0;k<len;k++) s+=a[idx[k]];
    var prev=s/len; out[idx[len-1]]=prev;
    for(var k=len;k<idx.length;k++){ var i=idx[k]; prev=al*a[i]+(1.0-al)*prev; out[i]=prev; }
    return out;
  }
  // fi = nz((close-close[1])*volume)
  var fi=new Array(n);
  for(var i=0;i<n;i++){
    if(i===0||closes[i]===null||closes[i-1]===null||vols[i]===null) fi[i]=0.0;
    else fi[i]=(closes[i]-closes[i-1])*vols[i];
  }
  // flow = cum - cum[vNet]  (trailing SUM of vNet fi)
  var cum=new Array(n), run=0.0;
  for(var i=0;i<n;i++){ run+=fi[i]; cum[i]=run; }
  var flow=new Array(n).fill(null);
  for(var i=0;i<n;i++){ if(i-vNet>=0) flow[i]=cum[i]-cum[i-vNet]; }
  // pre = WMA(flow, vPre)  (weights 1..vPre, newest = vPre)
  var pre=new Array(n).fill(null), denom=vPre*(vPre+1)/2;
  for(var i=0;i<n;i++){
    if(i-vPre+1<0) continue;
    var ok=true, sm=0.0;
    for(var k=0;k<vPre;k++){ var v=flow[i-vPre+1+k]; if(v===null){ok=false;break;} sm+=v*(k+1); }
    if(ok) pre[i]=sm/denom;
  }
  // fast = ZLEMA(pre, vFast) = EMA(2*pre - pre[1], vFast)
  var zin=new Array(n).fill(null);
  for(var i=1;i<n;i++) if(pre[i]!==null&&pre[i-1]!==null) zin[i]=2.0*pre[i]-pre[i-1];
  var fast=ema(zin,vFast);
  var slow=ema(pre,vSlow);
  var out=[];
  for(var i=0;i<n;i++){
    var p2;
    if(pre[i]===null || i<1 || pre[i-1]===null) p2=0;        // na(preV) or na(preV[1]) -> 0
    else if(fast[i]!==null && slow[i]!==null) p2=(fast[i]>slow[i])?1:-1;
    else p2=-1;                                              // fast/slow na, pre valid -> -1 (matches Pine)
    out.push({fast:fast[i], slow:slow[i], p2:p2});
  }
  return out;
""";


-- ========================= 2. BASE WEEKLY VIEWS =============================

-- v_sata_weekly: Mon-anchored weekly resample of price_history, DIVIDEND-ADJUSTED.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_weekly` AS
WITH d AS (
  SELECT
    ticker, date,
    SAFE_DIVIDE(adj_close, close) AS f,
    open, high, low, adj_close, volume
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date >= DATE '2000-01-01' AND date < CURRENT_DATE()
),
adj AS (
  SELECT
    ticker, date,
    open      * f AS aopen,
    high      * f AS ahigh,
    low       * f AS alow,
    adj_close      AS aclose,
    volume
  FROM d
  WHERE f IS NOT NULL          -- drops any row with bad/zero close
),
weekly AS (
  SELECT
    ticker,
    DATE_TRUNC(date, WEEK(MONDAY))                              AS wk,
    ARRAY_AGG(aopen  ORDER BY date ASC  LIMIT 1)[OFFSET(0)]     AS open,
    MAX(ahigh)                                                  AS high,
    MIN(alow)                                                   AS low,
    ARRAY_AGG(aclose ORDER BY date DESC LIMIT 1)[OFFSET(0)]     AS close,
    ARRAY_AGG(aclose ORDER BY date DESC LIMIT 1)[OFFSET(0)]     AS adj_close,
    SUM(volume)                                                 AS volume,
    MAX(date)                                                   AS last_trading_day,
    COUNT(*)                                                    AS days_in_week
  FROM adj
  GROUP BY ticker, wk
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY wk) AS wk_index
FROM weekly;


-- v_spx_weekly: Monday-anchored weekly CASH-SPX close, resampled from spx_daily.
-- v6d FIX: spx_daily MUST hold TVC:SPX cash (TVC_SPX__1D load) — NOT SPCFD, NO lag.
-- (SPCFD + a phantom 1-week lag was the old double-bug that broke Mansfield;
--  never reintroduce either.)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_spx_weekly` AS
SELECT
  DATE_TRUNC(dt, WEEK(MONDAY)) AS wk,
  ARRAY_AGG(close ORDER BY dt DESC LIMIT 1)[OFFSET(0)] AS spx_close   -- last close of week
FROM (
  SELECT DATE(TIMESTAMP_SECONDS(`time`)) AS dt, close
  FROM `stonks-498420.stonks_data.spx_daily`
)
GROUP BY wk;


-- ========================= 3. BAND VIEWS ====================================

-- Row 1 — Overhead Resistance (Ichimoku cloud, offset 26).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_01_overhead` AS
WITH base AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w9 AS hh9, MIN(low) OVER w9 AS ll9, COUNT(*) OVER w9 AS n9,
    MAX(high) OVER w26 AS hh26, MIN(low) OVER w26 AS ll26, COUNT(*) OVER w26 AS n26,
    MAX(high) OVER w52 AS hh52, MIN(low) OVER w52 AS ll52, COUNT(*) OVER w52 AS n52
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW w9 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 8 PRECEDING AND CURRENT ROW),
         w26 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
         w52 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
),
ich AS (
  SELECT ticker, wk, close,
    CASE WHEN n26>=26 THEN (hh26+ll26)/2 END AS kij,
    CASE WHEN n9>=9 AND n26>=26 THEN ((hh9+ll9)/2 + (hh26+ll26)/2)/2 END AS spanA_now,
    CASE WHEN n52>=52 THEN (hh52+ll52)/2 END AS spanB_now
  FROM base
),
d AS (
  SELECT ticker, wk, close, kij,
    LAG(spanA_now,26) OVER (PARTITION BY ticker ORDER BY wk) AS spanA,
    LAG(spanB_now,26) OVER (PARTITION BY ticker ORDER BY wk) AS spanB
  FROM ich
)
SELECT ticker, wk, close, kij, spanA, spanB,
  CASE
    WHEN spanA IS NULL OR spanB IS NULL THEN 0
    WHEN close>GREATEST(spanA,spanB) AND close>kij THEN 1
    WHEN ((spanB>spanA) AND NOT(close>GREATEST(spanA,spanB)) AND close<kij)
      OR (NOT(spanB>spanA) AND close<LEAST(spanA,spanB) AND close<kij) THEN -1
    ELSE 0
  END AS row1_overhead
FROM d;


-- Rows 6 & 7 — close vs SMA40 / SMA10.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0607_price_vs_ma` AS
WITH b AS (
  SELECT ticker, wk, close,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w40 AS sma40,
    COUNT(close) OVER w10 AS n10, COUNT(close) OVER w40 AS n40
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w40 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 39 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, close, sma10, sma40,
  CASE WHEN n40<40 THEN NULL WHEN close>sma40 THEN 1 ELSE -1 END AS row6_close_gt_40w,
  CASE WHEN n10<10 THEN NULL WHEN close>sma10 THEN 1 ELSE -1 END AS row7_close_gt_10w
FROM b;


-- Rows 8 & 9 — SMA30 / SMA10 slope (rising/falling/flat).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_0809_ma_slope` AS
WITH b AS (
  SELECT ticker, wk, wk_index,
    AVG(close) OVER w10 AS sma10, AVG(close) OVER w30 AS sma30
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW w10 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
         w30 AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
),
s AS (
  SELECT ticker, wk, wk_index, sma10, sma30,
    LAG(sma10) OVER (PARTITION BY ticker ORDER BY wk) AS sma10_prev,
    LAG(sma30) OVER (PARTITION BY ticker ORDER BY wk) AS sma30_prev
  FROM b
)
SELECT ticker, wk,
  CASE WHEN wk_index<31 THEN NULL WHEN sma30>sma30_prev THEN 1 WHEN sma30<sma30_prev THEN -1 ELSE 0 END AS row8_30w_rising,
  CASE WHEN wk_index<11 THEN NULL WHEN sma10>sma10_prev THEN 1 WHEN sma10<sma10_prev THEN -1 ELSE 0 END AS row9_10w_rising
FROM s;


-- Row 10 — Breakouts (Donchian 13W, event flash).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_10_breakout` AS
WITH b AS (
  SELECT ticker, wk, close,
    MAX(high) OVER w AS channel_hh_13w, MIN(low) OVER w AS channel_ll_13w, COUNT(*) OVER w AS n
  FROM `stonks-498420.stonks_data.v_sata_weekly`
  WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)
)
SELECT ticker, wk, close, channel_hh_13w, channel_ll_13w,
  CASE WHEN n<13 THEN 0 WHEN close>channel_hh_13w THEN 1 WHEN close<channel_ll_13w THEN -1 ELSE 0 END AS row10_breakout
FROM b;


-- Row 5 — Mansfield RS (close/SPX vs 52W avg).
-- v6d FIX: SAME-WEEK cash SPX, NO lag. (Old version lagged SPX 1 week via LAG()
--  on a SPCFD feed — that double-bug caused ~3% razor-thin off-by-1 flips.)
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_05_mansfield` AS
WITH j AS (
  SELECT eq.ticker, eq.wk,
    SAFE_DIVIDE(eq.close, spx.spx_close)*100 AS ratio
  FROM `stonks-498420.stonks_data.v_sata_weekly` eq
  LEFT JOIN `stonks-498420.stonks_data.v_spx_weekly` spx USING (wk)   -- SAME WEEK, no lag
),
m AS (
  SELECT ticker, wk, ratio,
    AVG(ratio) OVER w AS maRS, COUNT(ratio) OVER w AS n
  FROM j WINDOW w AS (PARTITION BY ticker ORDER BY wk ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
)
SELECT ticker, wk, ratio, maRS, ((ratio/maRS)-1)*100 AS mansfield_line,
  CASE WHEN n<52 THEN NULL WHEN ratio>maRS THEN 1 ELSE -1 END AS row5_mansfield
FROM m;


-- Row 3 — MACD line vs signal (warmup wk_index < 34).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_03_macd` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(wk ORDER BY wk) AS wks, ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly` GROUP BY ticker
),
calc AS (SELECT ticker, wks, idxs, `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].macd AS macd, arr[OFFSET(i)].signal AS signal, arr[OFFSET(i)].hist AS hist,
  CASE WHEN idxs[OFFSET(i)]<34 THEN NULL WHEN arr[OFFSET(i)].macd>arr[OFFSET(i)].signal THEN 1 ELSE -1 END AS row3_macd_gt_signal
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i;


-- Row 4 — Elder Impulse (EMA13 slope + MACD-hist slope; warmup wk_index < 35).
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_04_elder` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(wk ORDER BY wk) AS wks, ARRAY_AGG(wk_index ORDER BY wk) AS idxs
  FROM `stonks-498420.stonks_data.v_sata_weekly` GROUP BY ticker
),
calc AS (
  SELECT ticker, wks, idxs,
    `stonks-498420.stonks_data.sata_ema`(closes,13) AS ema13,
    `stonks-498420.stonks_data.sata_macd`(closes,12,26,9) AS marr
  FROM series
),
r AS (
  SELECT ticker, wks[OFFSET(i)] AS wk, idxs[OFFSET(i)] AS idx,
         ema13[OFFSET(i)] AS ema13, marr[OFFSET(i)].hist AS hist
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks)-1)) AS i
)
SELECT ticker, wk, ema13, hist,
  CASE WHEN idx<35 THEN NULL
       ELSE ((CASE WHEN ema13>LAG(ema13) OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)
           + (CASE WHEN hist >LAG(hist)  OVER (PARTITION BY ticker ORDER BY wk) THEN 1 ELSE 0 END)) - 1
  END AS row4_elder
FROM r;


-- Row 2 — Volume (Force-Index money-flow crossover). 29-bar IPO warmup gate.
CREATE OR REPLACE VIEW `stonks-498420.stonks_data.band_02_volume` AS
WITH series AS (
  SELECT ticker, ARRAY_AGG(close ORDER BY wk) AS closes,
         ARRAY_AGG(CAST(volume AS FLOAT64) ORDER BY wk) AS vols, ARRAY_AGG(wk ORDER BY wk) AS wks
  FROM `stonks-498420.stonks_data.v_sata_weekly` GROUP BY ticker
),
calc AS (SELECT ticker, wks, `stonks-498420.stonks_data.sata_volume`(closes,vols,13,4,3,13) AS arr FROM series)
SELECT ticker, wks[OFFSET(i)] AS wk,
  arr[OFFSET(i)].fast AS vol_fast, arr[OFFSET(i)].slow AS vol_slow,
  CASE WHEN i < 29 THEN 0 ELSE arr[OFFSET(i)].p2 END AS row2_volume   -- 29-bar IPO warmup
FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks) - 1)) AS i;


-- ========================= 4. SATA SCORE ====================================
-- sata_score = count of GREEN (p==1) bands across all 10.

CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_sata_score` AS
SELECT w.ticker, w.wk,
    (CASE WHEN b01.row1_overhead=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b02.row2_volume=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b03.row3_macd_gt_signal=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b04.row4_elder=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b05.row5_mansfield=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row6_close_gt_40w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b67.row7_close_gt_10w=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row8_30w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b89.row9_10w_rising=1 THEN 1 ELSE 0 END)
  + (CASE WHEN b10.row10_breakout=1 THEN 1 ELSE 0 END) AS sata_score,
  b01.row1_overhead, b02.row2_volume, b03.row3_macd_gt_signal, b04.row4_elder,
  b05.row5_mansfield, b67.row6_close_gt_40w, b67.row7_close_gt_10w,
  b89.row8_30w_rising, b89.row9_10w_rising, b10.row10_breakout
FROM `stonks-498420.stonks_data.v_sata_weekly` w
LEFT JOIN `stonks-498420.stonks_data.band_01_overhead`      b01 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_02_volume`        b02 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_03_macd`          b03 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_04_elder`         b04 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_05_mansfield`     b05 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_0607_price_vs_ma` b67 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_0809_ma_slope`    b89 USING (ticker, wk)
LEFT JOIN `stonks-498420.stonks_data.band_10_breakout`      b10 USING (ticker, wk);

-- ============================================================================
--  END. After running: SELECT * FROM stonks_data.v_sata_score LIMIT 10;
-- ============================================================================
