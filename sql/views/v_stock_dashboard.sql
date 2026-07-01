CREATE OR REPLACE VIEW `stonks-498420.stonks_data.v_stock_dashboard` AS
WITH d AS (
  SELECT m.*,
    s.main_theme, s.sub_theme, s.etf,
    ROUND((m.ret_1d+m.ret_1w+m.ret_1m+m.ret_3m+m.ret_6m)/NULLIF(m.atr_pct,0),4) AS group_score
  FROM `stonks-498420.stonks_data.metrics_daily` m
  LEFT JOIN `stonks-498420.stonks_data.stock_theme_map` s USING (ticker)
)
SELECT
  d.*,
  t.name, t.sector, t.industry,
  CASE t.sector
    WHEN 'Technology' THEN 'XLK' WHEN 'Healthcare' THEN 'XLV'
    WHEN 'Financial Services' THEN 'XLF' WHEN 'Consumer Cyclical' THEN 'XLY'
    WHEN 'Consumer Defensive' THEN 'XLP' WHEN 'Energy' THEN 'XLE'
    WHEN 'Industrials' THEN 'XLI' WHEN 'Basic Materials' THEN 'XLB'
    WHEN 'Real Estate' THEN 'XLRE' WHEN 'Utilities' THEN 'XLU'
    WHEN 'Communication Services' THEN 'XLC' ELSE NULL
  END AS sector_etf,
  rm.rs_today, rm.rs_1d, rm.rs_3d, rm.rs_7d, rm.rs_14d,
  rm.rs_1d_3d, rm.rs_3d_7d, rm.rs_7d_14d,
  rm.rs_wow, rm.rs_wow_diff,
  rm.abs_rs_14d, rm.bs_ratio_over_ma, rm.bt_ma_rising,
  rm.pctile_1d, rm.pctile_3d, rm.pctile_7d, rm.short_pctile,
  rm.anchor_pctile, rm.today_pctile,
  ts.theme_rs, ts.avg_group_score AS theme_avg_group_score,
  ts.members AS theme_members, ts.upper_range AS theme_upper_range,
  ts.lower_range AS theme_lower_range, ts.group_cohesion AS theme_group_cohesion,
  CASE WHEN d.group_score IS NULL OR d.sub_theme IS NULL THEN NULL
       WHEN d.group_score > ts.upper_range THEN 'Leader'
       WHEN d.group_score < ts.lower_range THEN 'Laggard'
       ELSE 'Mid' END AS theme_rank,
  mc.* EXCEPT(ticker),
  trm.* EXCEPT(theme),
  (ROW_NUMBER() OVER (PARTITION BY d.ticker ORDER BY ts.theme_rs DESC) = 1) AS is_primary_theme,
  CASE WHEN se.broad_stage = 'N' THEN 'Neutral' ELSE se.broad_stage END AS broad_stage,
  se.stage, se.came_from, se.wks_since, se.stage_week, se.sata_w, se.mansfield_w,
  sd.sata_d, sd.score_chg_d,
  se.sma_10w, se.sma_30w, se.sma_40w,
  se.pct_to_10w, se.pct_to_30w, se.pct_to_40w,
  se.abv_10w, se.abv_30w, se.abv_40w,
  se.close_w, se.high_w, se.low_w, se.volume_w,
  se.stage_changed, se.substage_changed,
  se.wcr_pct,
  dcr.dcr_pct,
  hl.high_52w, hl.low_52w, hl.pct_from_52w_high, hl.pct_from_52w_low,
  se.channel_hh_52w, se.channel_ll_52w, se.channel_hc_52w, se.channel_lc_52w,
  se.channel_hh_26w, se.channel_ll_26w, se.channel_hc_26w, se.channel_lc_26w,
  se.channel_hh_13w, se.channel_ll_13w, se.channel_hc_13w, se.channel_lc_13w,
  se.channel_hh_4w,  se.channel_ll_4w,  se.channel_hc_4w,  se.channel_lc_4w,
  se.dollar_vol_w, se.avg_dollar_vol_10w, se.avg_vol_10w, se.rel_vol_w,
  se.last_s2_price, se.pct_since_s2, se.last_s4_price, se.pct_since_s4,
  gap.* EXCEPT(ticker),
  bo.* EXCEPT(ticker)
FROM d
LEFT JOIN `stonks-498420.stonks_data.tickers` t USING (ticker)
LEFT JOIN `stonks-498420.stonks_data.rs_momentum` rm USING (ticker)
LEFT JOIN `stonks-498420.stonks_data.theme_stats` ts ON ts.theme = d.sub_theme
LEFT JOIN `stonks-498420.stonks_data.ma_crosses` mc USING (ticker)
LEFT JOIN `stonks-498420.stonks_data.theme_rs_momentum` trm ON trm.theme = d.sub_theme
LEFT JOIN (
  SELECT se.ticker, se.broad_stage, se.stage, se.came_from, se.wks_since,
         se.wk AS stage_week, ss.sata_score AS sata_w,
         wd.mansfield_w, wd.sma_10w, wd.sma_30w, wd.sma_40w,
         wd.pct_to_10w, wd.pct_to_30w, wd.pct_to_40w,
         wd.abv_10w, wd.abv_30w, wd.abv_40w,
         wd.close_w, wd.high_w, wd.low_w, wd.volume_w,
         se.stage_changed, se.substage_changed,
         ROUND(SAFE_DIVIDE(wd.close_w - wd.low_w, wd.high_w - wd.low_w), 4) AS wcr_pct,
         wd.channel_hh_52w, wd.channel_ll_52w, wd.channel_hc_52w, wd.channel_lc_52w,
         wd.channel_hh_26w, wd.channel_ll_26w, wd.channel_hc_26w, wd.channel_lc_26w,
         wd.channel_hh_13w, wd.channel_ll_13w, wd.channel_hc_13w, wd.channel_lc_13w,
         wd.channel_hh_4w,  wd.channel_ll_4w,  wd.channel_hc_4w,  wd.channel_lc_4w,
         wd.dollar_vol_w, wd.avg_dollar_vol_10w, wd.avg_vol_10w, wd.rel_vol_w,
         wd.last_s2_price, wd.pct_since_s2, wd.last_s4_price, wd.pct_since_s4
  FROM `stonks-498420.stonks_data.stage_engine` se
  JOIN `stonks-498420.stonks_data.sata_score` ss ON ss.ticker = se.ticker AND ss.wk = se.wk
  LEFT JOIN `stonks-498420.stonks_data.weekly_detail` wd ON wd.ticker = se.ticker AND wd.wk = se.wk
  QUALIFY ROW_NUMBER() OVER (PARTITION BY se.ticker ORDER BY se.wk DESC) = 1
) se USING (ticker)
LEFT JOIN (
  SELECT ticker, sata_score_d AS sata_d,
         sata_score_d - LAG(sata_score_d) OVER (PARTITION BY ticker ORDER BY date) AS score_chg_d
  FROM `stonks-498420.stonks_data.sata_score_daily`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
) sd USING (ticker)
LEFT JOIN (
  SELECT ticker, ROUND(SAFE_DIVIDE(close - low, high - low), 4) AS dcr_pct
  FROM `stonks-498420.stonks_data.price_history`
  WHERE date < CURRENT_DATE()
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
) dcr USING (ticker)
LEFT JOIN (
  SELECT agg.ticker,
    ROUND(agg.high_52w, 2) AS high_52w, ROUND(agg.low_52w, 2) AS low_52w,
    ROUND(lp.last_close / NULLIF(agg.high_52w, 0) - 1, 4) AS pct_from_52w_high,
    ROUND(lp.last_close / NULLIF(agg.low_52w, 0)  - 1, 4) AS pct_from_52w_low
  FROM (
    SELECT ticker, MAX(high) AS high_52w, MIN(low) AS low_52w
    FROM `stonks-498420.stonks_data.price_history`
    WHERE date < CURRENT_DATE() AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 364 DAY)
    GROUP BY ticker
  ) agg
  JOIN (
    SELECT ticker, close AS last_close
    FROM `stonks-498420.stonks_data.price_history`
    WHERE date < CURRENT_DATE() AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 364 DAY)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
  ) lp USING (ticker)
) hl USING (ticker)
LEFT JOIN (
  SELECT ticker,
    support_gaps_near, resistance_gaps_near, net_gap_bias_near,
    in_support_gap, in_resistance_gap,
    nearest_support_top, pct_to_support, nearest_resistance_bottom, pct_to_resistance,
    days_since_gap_up, days_since_gap_down,
    nearest_support_base, nearest_support_shakeout, nearest_support_wedge_pop, nearest_support_earnings,
    near_support_base_n, near_support_shakeout_n, near_support_wedge_pop_n, near_support_earnings_n
  FROM `stonks-498420.stonks_data.gap_features`
  WHERE tf = 'D'
) gap USING (ticker)
LEFT JOIN (
  SELECT ticker,
    bo_daily, bo_weekly, days_since_bo_daily, days_since_bo_weekly,
    in_watch_daily, in_watch_weekly, in_watch, pullback_entry,
    pct_to_21ema AS bo_pct_to_21ema
  FROM `stonks-498420.stonks_data.breakout_features`
) bo USING (ticker)
