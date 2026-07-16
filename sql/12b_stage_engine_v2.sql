-- 12b_stage_engine_v2.sql
-- PARALLEL v2 stage engine (geometry broad rule). Writes `stage_engine_v2` and does NOT
-- touch the live `stage_engine`. Lets setups be compared under v1 vs v2 before any swap.
--
-- v2 BROAD change vs v1: drops the r8 30W-SLOPE exit. A stage holds while price stays on its
-- side of the 30W/40W and goes Neutral when price CROSSES the MA; entry needs 3% separation
-- from the 30W. 1:1 port of the Python validated bit-exact on 2 settled sas weeks
-- (worst-week 83.6% -> 91.7%). Substage + lineage clock IDENTICAL to v1 (B>=41, minus via
-- row7, Neutral 1/3, lineage clock — unchanged).
--
-- Reads sata_score (11) + weekly_geometry (11b). Runs after both (filename sorts 12_ < 12b_).
-- NOTE: pct arrays are COALESCE'd to 0.0 only on warmup weeks (valids=0 there, so the
-- sentinel is never read by the walk). valids = all-10-bands-present, identical to v1/Python.

CREATE OR REPLACE FUNCTION `stonks-498420`.stonks_data.stage_walk_v2(
  scores ARRAY<FLOAT64>, valids ARRAY<FLOAT64>,
  pct30s ARRAY<FLOAT64>, pct40s ARRAY<FLOAT64>
)
RETURNS ARRAY<STRUCT<broad STRING, came_from STRING, wks_since INT64>>
LANGUAGE js AS r"""
  var n = scores.length;
  var out = [];
  var st = "N";
  var lineage = null;
  var clock = 0;
  var S2E = 7, S2X = 2, S4E = 1, S4X = 6, SEP = 0.03;   // validated thresholds
  for (var i = 0; i < n; i++){
    if (Number(valids[i]) === 1){
      var s   = Number(scores[i]);
      var p30 = (pct30s[i] === null) ? null : Number(pct30s[i]);
      var p40 = (pct40s[i] === null) ? null : Number(pct40s[i]);
      var sep2  = (p30 !== null && p30 <= -SEP);                          // price >=3% ABOVE 30W
      var sep4  = (p30 !== null && p30 >=  SEP);                          // price >=3% BELOW 30W
      var exit2 = (p30 !== null && p30 > 0) || (p40 !== null && p40 > 0); // fell below 30W or 40W
      var exit4 = (p30 !== null && p30 < 0) || (p40 !== null && p40 < 0); // rose above 30W or 40W
      if (st === "S2"){
        if (s <= S4E) st = "S4";
        else if (s < S2X || exit2) st = "N";
      } else if (st === "S4"){
        if (s >= S2E && sep2) st = "S2";
        else if (s > S4X || exit4) st = "N";
      } else {
        if (s >= S2E && sep2) st = "S2";
        else if (s <= S4E && sep4) st = "S4";
      }
    }
    // lineage clock — reset to 1 ONLY on an S2<->S4 flip; count through Neutral (same as v1)
    if (st === "S2"){
      if (lineage !== "S2"){ lineage = "S2"; clock = 1; } else clock += 1;
    } else if (st === "S4"){
      if (lineage !== "S4"){ lineage = "S4"; clock = 1; } else clock += 1;
    } else {
      if (lineage !== null) clock += 1;
    }
    out.push({broad: st, came_from: lineage, wks_since: clock});
  }
  return out;
""";

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.stage_engine_v2` CLUSTER BY ticker AS
WITH base AS (
  SELECT
    s.ticker, s.wk,
    CAST(s.sata_score AS FLOAT64) AS score,
    s.row7_close_gt_10w           AS row7,
    CASE WHEN s.row1_overhead      IS NOT NULL AND s.row2_volume        IS NOT NULL
          AND s.row3_macd_gt_signal IS NOT NULL AND s.row4_elder         IS NOT NULL
          AND s.row5_mansfield      IS NOT NULL AND s.row6_close_gt_40w  IS NOT NULL
          AND s.row7_close_gt_10w   IS NOT NULL AND s.row8_30w_rising    IS NOT NULL
          AND s.row9_10w_rising     IS NOT NULL AND s.row10_breakout     IS NOT NULL
         THEN 1.0 ELSE 0.0 END     AS valid,
    COALESCE(g.pct_to_30w, 0.0)    AS pct30,
    COALESCE(g.pct_to_40w, 0.0)    AS pct40
  FROM `stonks-498420.stonks_data.sata_score` s
  LEFT JOIN `stonks-498420.stonks_data.weekly_geometry` g USING (ticker, wk)
),
series AS (
  SELECT ticker,
    ARRAY_AGG(score ORDER BY wk) AS scores,
    ARRAY_AGG(valid ORDER BY wk) AS valids,
    ARRAY_AGG(pct30 ORDER BY wk) AS pct30s,
    ARRAY_AGG(pct40 ORDER BY wk) AS pct40s,
    ARRAY_AGG(row7  ORDER BY wk) AS r7s,
    ARRAY_AGG(wk    ORDER BY wk) AS wks
  FROM base
  GROUP BY ticker
),
calc AS (
  SELECT ticker, wks, r7s,
    `stonks-498420.stonks_data.stage_walk_v2`(scores, valids, pct30s, pct40s) AS w
  FROM series
),
flat AS (
  SELECT ticker,
    wks[OFFSET(i)]         AS wk,
    w[OFFSET(i)].broad     AS broad_stage,
    w[OFFSET(i)].came_from AS came_from,
    w[OFFSET(i)].wks_since AS wks_since,
    r7s[OFFSET(i)]         AS row7
  FROM calc, UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(wks) - 1)) AS i
),
labeled AS (
  SELECT ticker, wk, broad_stage, came_from, wks_since,
    CASE
      WHEN broad_stage = 'S2' THEN CONCAT('2',
           CASE WHEN wks_since = 1 THEN 'A' WHEN wks_since >= 41 THEN 'B' ELSE '' END,
           CASE WHEN row7 = -1 THEN '-' ELSE '' END)
      WHEN broad_stage = 'S4' THEN CONCAT('4',
           CASE WHEN wks_since = 1 THEN 'A' WHEN wks_since >= 41 THEN 'B' ELSE '' END,
           CASE WHEN row7 =  1 THEN '-' ELSE '' END)
      WHEN broad_stage = 'N' AND came_from = 'S2' THEN '3'
      WHEN broad_stage = 'N' AND came_from = 'S4' THEN '1'
      ELSE 'N'
    END AS stage
  FROM flat
)
SELECT ticker, wk, broad_stage, came_from, wks_since, stage,
  broad_stage <> LAG(broad_stage) OVER (PARTITION BY ticker ORDER BY wk) AS stage_changed,
  stage       <> LAG(stage)       OVER (PARTITION BY ticker ORDER BY wk) AS substage_changed
FROM labeled;
