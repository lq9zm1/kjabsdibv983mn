-- 12_stage_engine.sql
-- Broad stage (S2/Neutral/S4) + resolved substage + came-from + weeks-since-anchor.
-- Reads the sata_score TABLE (built by 11_sata_score.sql, runs immediately before).
-- Self-contained: defines the UDF and builds the table. Idempotent (CREATE OR REPLACE).
-- Validated vs stageanalysis.net 2026-06-08: broad 88.57%, substage 88.49% (agreeing),
--   end-to-end 79.94%. The ~89% broad ceiling is a proven OHLCV-information limit.
--
-- Clock rule (validated 50.2% vs sas's published Wks Since, beating reset-on-reentry 32.4%):
--   wks_since resets to 1 (new "A") ONLY on a lineage flip S2<->S4; Neutral keeps the
--   prior lineage and keeps counting (sas Neutral clock reaches 190 -> persists through N).

CREATE OR REPLACE FUNCTION `stonks-498420`.stonks_data.stage_walk(
  scores ARRAY<FLOAT64>, r8s ARRAY<FLOAT64>, valids ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<broad STRING, came_from STRING, wks_since INT64>> LANGUAGE js AS r"""
  var n = scores.length;
  var out = [];
  var st = "N";
  var lineage = null;   // last non-Neutral broad stage
  var clock = 0;        // weeks since current lineage anchor
  for (var i = 0; i < n; i++){
    if (Number(valids[i]) === 1){
      var s = Number(scores[i]);
      var slope = (r8s[i] === null) ? null : Number(r8s[i]);
      if (st === "S2"){
        if (s <= 1) st = "S4";
        else if (s < 3 || slope === -1) st = "N";
      } else if (st === "S4"){
        if (s >= 8) st = "S2";
        else if (s > 5 || slope === 1) st = "N";
      } else {
        if (s >= 8) st = "S2";
        else if (s <= 1) st = "S4";
      }
    }
    if (st === "S2"){
      if (lineage !== "S2"){ lineage = "S2"; clock = 1; }
      else clock += 1;
    } else if (st === "S4"){
      if (lineage !== "S4"){ lineage = "S4"; clock = 1; }
      else clock += 1;
    } else {
      if (lineage !== null) clock += 1;
    }
    out.push({broad: st, came_from: lineage, wks_since: clock});
  }
  return out;
""";

CREATE OR REPLACE TABLE `stonks-498420.stonks_data.stage_engine` CLUSTER BY ticker AS
WITH series AS (
  SELECT ticker,
    ARRAY_AGG(CAST(sata_score      AS FLOAT64) ORDER BY wk) AS scores,
    ARRAY_AGG(CAST(row8_30w_rising AS FLOAT64) ORDER BY wk) AS r8s,
    ARRAY_AGG(row7_close_gt_10w               ORDER BY wk) AS r7s,
    ARRAY_AGG(wk                              ORDER BY wk) AS wks,
    ARRAY_AGG(
      CASE WHEN row1_overhead      IS NOT NULL AND row2_volume        IS NOT NULL
            AND row3_macd_gt_signal IS NOT NULL AND row4_elder         IS NOT NULL
            AND row5_mansfield      IS NOT NULL AND row6_close_gt_40w  IS NOT NULL
            AND row7_close_gt_10w   IS NOT NULL AND row8_30w_rising    IS NOT NULL
            AND row9_10w_rising     IS NOT NULL AND row10_breakout     IS NOT NULL
           THEN 1.0 ELSE 0.0 END
      ORDER BY wk) AS valids
  FROM `stonks-498420.stonks_data.sata_score`
  GROUP BY ticker
),
calc AS (
  SELECT ticker, wks, r7s,
    `stonks-498420.stonks_data.stage_walk`(scores, r8s, valids) AS w
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
)
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
FROM flat;
