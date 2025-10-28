BEGIN
  CREATE TEMP TABLE base_all AS
  
  WITH monthly_chain_activity AS (
    SELECT 
      DATE_TRUNC(dt,MONTH) AS dt_month, chain_key, display_name,
      -- attributes
      layer, eth_eco_l2, alignment, provider_entity_w_superchain, is_evm, gas_token, da_layer, output_root_layer, l2b_stage
      -- value metrics
      , MAX_BY(app_tvl_usd,dt) AS latest_app_tvl_usd
      , MAX_BY(stables_onchain_usd,dt) AS latest_stables_onchain_usd
      , MAX_BY(onchain_value_usd,dt) AS latest_onchain_value_usd
      -- activity metrics
      , SUM(txs_per_day) as sum_txs
      , SUM(total_rev_fees_eth) AS sum_total_rev_txn_fees_eth
      , SUM(total_rev_fees_usd) AS sum_total_rev_txn_fees_usd
      , SUM(total_dex_volume_usd) as sum_total_dex_volume_usd
      -- developers
      , MAX_BY(distinct_parent_protocol,dt) AS latest_distinct_parent_protocol
      -- -- UX
      -- , SUM(median_l2_usd_fees_per_tx*txs_per_day)/SUM(txs_per_day) AS wt_avg_median_l2_usd_fees_per_tx
    FROM `oplabs-tools-data.materialized_tables.daily_superchain_health_mv`
      WHERE dt >= '2021-01-01'
          AND dt < DATE_TRUNC(CURRENT_DATE(),MONTH)

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  )
  , days_per_mo AS (
      SELECT DATE_TRUNC(dt,MONTH) AS dt_month, COUNT(DISTINCT dt) AS unique_dt
      FROM `oplabs-tools-data.materialized_tables.daily_superchain_health_mv`
      GROUP BY 1 
  )
  , monthly_revshare AS (
    SELECT 
    dt_month, display_name, 'L2' AS layer, 'OP Chain' as alignment
    -- revenue
      , revshare_estimated AS sum_revshare_estimated_eth
      , revshare_estimated_usd AS sum_revshare_estimated_usd
      , actual_collective_contribution AS sum_revshare_actual_eth
      , actual_collective_contribution_usd AS sum_revshare_actual_usd
      , actual_collective_contribution_rec AS sum_actual_collective_contribution_received_eth
      , actual_collective_contribution_rec_usd AS sum_actual_collective_contribution_received_usd
      FROM `oplabs-tools-data.materialized_tables.monthly_opcollective_revshare_mv`
    WHERE 1=1
      AND (
        (is_flagged IS NULL AND prior_flag IS NULL)
        OR
        display_name LIKE 'RaaS -%'
        )
      AND chart_name != 'Unmapped'
      AND is_flagged IS NULL
      AND alignment = 'OP Chain'
      AND dt_month >= '2021-01-01'
          AND dt_month < DATE_TRUNC(CURRENT_DATE(),MONTH)
  )
  , monthly_chain_level AS (
    SELECT
        COALESCE(a.dt_month,r.dt_month) AS dt_month
      , COALESCE(a.display_name, r.display_name) AS display_name
      , COALESCE(a.chain_key,lower(coalesce(a.display_name,r.display_name))) AS chain_key
      , COALESCE(a.layer, r.layer) AS layer
      , COALESCE(a.alignment, r.alignment) AS alignment
      , CASE WHEN r.display_name IS NOT NULL THEN 'Optimism: Superchain'
          ELSE a.provider_entity_w_superchain
          END AS provider_entity_w_superchain
      , a.* EXCEPT(dt_month, display_name, layer, alignment,chain_key, provider_entity_w_superchain)
      , r.* EXCEPT(dt_month, display_name, layer, alignment)
      , d.unique_dt
    FROM monthly_chain_activity a 
    LEFT JOIN days_per_mo d
      ON a.dt_month = d.dt_month
    FULL OUTER JOIN monthly_revshare r
      ON a.dt_month = r.dt_month
      AND a.display_name = r.display_name
  )
  ,pre_aggregate_groups AS (
    SELECT *, COALESCE(
          CASE WHEN provider_entity_w_superchain IN ('Optimism: Superchain','Polygon: CDK', 'Arbitrum: Orbit', 'zkSync: ZK Stack') THEN provider_entity_w_superchain
          ELSE NULL
          END
          , 'Other') AS agg_display_name
    FROM monthly_chain_level
  )
  , aggregates AS (
    SELECT dt_month, agg_display_name, display_name, layer, eth_eco_l2,
      SUM(latest_app_tvl_usd) AS latest_app_tvl_usd,
      SUM(latest_stables_onchain_usd) AS latest_stables_onchain_usd,
      SUM(latest_onchain_value_usd) AS latest_onchain_value_usd,
      SUM(sum_txs) AS sum_txs,
        SUM(sum_txs/unique_dt) AS sum_txs_per_day,
        SUM(sum_txs/(unique_dt*(60*60*24))) AS sum_txs_per_sec,
      SUM(sum_total_dex_volume_usd) AS sum_dex_volume_usd,
        SUM(sum_total_dex_volume_usd/unique_dt) AS sum_dex_volume_usd_per_day,
      SUM(sum_total_rev_txn_fees_eth) AS sum_total_rev_txn_fees_eth,
        SUM(sum_total_rev_txn_fees_eth/unique_dt) AS sum_total_rev_txn_fees_eth_per_day,
      SUM(sum_total_rev_txn_fees_usd) AS sum_total_rev_txn_fees_usd,
        SUM(sum_total_rev_txn_fees_usd/unique_dt) AS sum_total_rev_txn_fees_usd_per_day,
      SUM(sum_revshare_estimated_eth) AS sum_revshare_estimated_eth,
        SUM(sum_revshare_estimated_eth/unique_dt) AS sum_revshare_estimated_eth_per_day,
      SUM(sum_revshare_estimated_usd) AS sum_revshare_estimated_usd,
        SUM(sum_revshare_estimated_usd/unique_dt) AS sum_revshare_estimated_usd_per_day,
      SUM(sum_revshare_actual_eth) AS sum_revshare_actual_eth,
        SUM(sum_revshare_actual_eth/unique_dt) AS sum_revshare_actual_eth_per_day,
      SUM(sum_revshare_actual_usd) AS sum_revshare_actual_usd,
        SUM(sum_revshare_actual_usd/unique_dt) AS sum_revshare_actual_usd_per_day
      -- SUM(wt_avg_median_l2_usd_fees_per_tx*sum_txs)/SUM(sum_txs) AS wt_avg_median_l2_usd_fees_per_tx

    FROM pre_aggregate_groups
    GROUP BY 1,2,3,4,5
  )
  SELECT *
    , dense_rank() over (order by dt_month DESC) AS month_recency_rank
  FROM aggregates
  ORDER BY dt_month DESC;

  CREATE TEMP TABLE base AS
    SELECT * EXCEPT (layer, eth_eco_l2)
    FROM base_all
    WHERE 
      layer = 'L2' -- L2s Only
      AND eth_eco_l2 -- Ethereum Ecosystem Only (e.g., no known BNB L2s)
  ;

  CREATE TEMP TABLE chain_measurements AS
  WITH cte AS (
    SELECT
      display_name,
      SUM(IFNULL(sum_revshare_estimated_eth, 0)) AS estimated_eth,
      SUM(IFNULL(sum_revshare_actual_eth, 0)) AS actual_eth
    FROM base
    WHERE agg_display_name = 'Optimism: Superchain'
    GROUP BY 1
  )
  SELECT
    display_name,
    STRUCT(
      estimated_eth,
      actual_eth
    ) AS all_time_revshare
  FROM cte
  ;
  CREATE TEMP TABLE rainbow_figure AS
  WITH cte AS (
  SELECT
      dt_month,
      agg_display_name,
      SUM(sum_total_rev_txn_fees_usd) AS sum_total_rev_txn_fees_usd,
      SUM(sum_txs_per_day) AS sum_txs_per_day
    FROM base
    GROUP BY 1, 2
  )
  SELECT
    agg_display_name,
    ARRAY_AGG(
      STRUCT(dt_month, sum_total_rev_txn_fees_usd AS txn_fees_usd, sum_txs_per_day AS txs_per_day)
      ORDER BY dt_month DESC
    ) AS timeline
  FROM cte
  WHERE dt_month > '2024-06-01'
  GROUP BY 1;
  CREATE TEMP TABLE tiles AS
  WITH tot_superchain AS (
    SELECT
      dt_month,
      SUM(IFNULL(sum_txs, 0)) AS sum_txs,
      SUM(IFNULL(sum_txs_per_day, 0)) AS sum_txs_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_usd, 0)) AS sum_total_rev_txn_fees_usd,
      SUM(IFNULL(sum_total_rev_txn_fees_eth, 0)) AS sum_total_rev_txn_fees_eth,
      SUM(IFNULL(sum_total_rev_txn_fees_usd_per_day, 0)) AS sum_total_rev_txn_fees_usd_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_eth_per_day, 0)) AS sum_total_rev_txn_fees_eth_per_day,
      SUM(IFNULL(sum_dex_volume_usd_per_day, 0)) AS sum_dex_volume_usd_per_day,
      SUM(IFNULL(latest_app_tvl_usd, 0)) AS latest_app_tvl_usd,
      SUM(IFNULL(latest_onchain_value_usd, 0)) AS latest_onchain_value_usd,
      SUM(IFNULL(latest_stables_onchain_usd, 0)) AS latest_stables_onchain_usd,
      SUM(IFNULL(sum_revshare_estimated_eth, 0)) AS sum_revshare_estimated_eth,
      SUM(IFNULL(sum_revshare_estimated_usd, 0)) AS sum_revshare_estimated_usd,
      SUM(IFNULL(sum_revshare_actual_eth, 0)) AS sum_revshare_actual_eth,
      SUM(IFNULL(sum_revshare_actual_usd, 0)) AS sum_revshare_actual_usd,
    FROM base
    WHERE agg_display_name = 'Optimism: Superchain'
    GROUP BY 1
  ),
  tot_eco AS (
    SELECT
      dt_month,
      SUM(IFNULL(sum_txs, 0)) AS sum_txs,
      SUM(IFNULL(sum_total_rev_txn_fees_usd, 0)) AS sum_total_rev_txn_fees_usd,
    FROM base
    GROUP BY 1
  ),
  cte AS (
    SELECT
      dt_month,
      SAFE_DIVIDE(ts.sum_total_rev_txn_fees_usd, te.sum_total_rev_txn_fees_usd) AS ecosystem_pct_of_l2_rev_fees_usd,
      SAFE_DIVIDE(ts.sum_total_rev_txn_fees_usd, te.sum_total_rev_txn_fees_usd)
        - LAG(SAFE_DIVIDE(ts.sum_total_rev_txn_fees_usd, te.sum_total_rev_txn_fees_usd), 1) OVER w AS mom_ecosystem_pct_of_l2_rev_fees_usd,
      SAFE_DIVIDE(ts.sum_txs, te.sum_txs) AS ecosystem_pct_of_l2_txs,
      SAFE_DIVIDE(ts.sum_txs, te.sum_txs) - LAG(SAFE_DIVIDE(ts.sum_txs, te.sum_txs), 1) OVER w AS mom_ecosystem_pct_of_l2_txs,
      ts.sum_total_rev_txn_fees_usd_per_day,
      ts.sum_total_rev_txn_fees_eth_per_day,
      SAFE_DIVIDE(
        ts.sum_total_rev_txn_fees_usd_per_day - LAG(ts.sum_total_rev_txn_fees_usd_per_day, 1) OVER w,
        LAG(ts.sum_total_rev_txn_fees_usd_per_day, 1) OVER w
      ) AS mom_sum_total_rev_txn_fees_usd_per_day,
      SAFE_DIVIDE(
        ts.sum_total_rev_txn_fees_eth_per_day - LAG(ts.sum_total_rev_txn_fees_eth_per_day, 1) OVER w,
        LAG(ts.sum_total_rev_txn_fees_eth_per_day, 1) OVER w
      ) AS mom_sum_total_rev_txn_fees_eth_per_day,
      ts.sum_dex_volume_usd_per_day,
      SAFE_DIVIDE(
        ts.sum_dex_volume_usd_per_day - LAG(ts.sum_dex_volume_usd_per_day, 1) OVER w,
        LAG(ts.sum_dex_volume_usd_per_day, 1) OVER w
      ) AS mom_sum_dex_volume_usd_per_day,
      ts.sum_txs_per_day,
      SAFE_DIVIDE(
        ts.sum_txs_per_day - LAG(ts.sum_txs_per_day, 1) OVER w,
        LAG(ts.sum_txs_per_day, 1) OVER w
      ) AS mom_sum_txs_per_day,
      ts.latest_app_tvl_usd,
      SAFE_DIVIDE(
        ts.latest_app_tvl_usd - LAG(ts.latest_app_tvl_usd, 1) OVER w,
        LAG(ts.latest_app_tvl_usd, 1) OVER w
      ) AS mom_latest_app_tvl_usd,
      ts.latest_onchain_value_usd,
      SAFE_DIVIDE(
        ts.latest_onchain_value_usd - LAG(ts.latest_onchain_value_usd, 1) OVER w,
        LAG(ts.latest_onchain_value_usd, 1) OVER w
      ) AS mom_latest_onchain_value_usd,
      ts.latest_stables_onchain_usd,
      SAFE_DIVIDE(
        ts.latest_stables_onchain_usd - LAG(ts.latest_stables_onchain_usd, 1) OVER w,
        LAG(ts.latest_stables_onchain_usd, 1) OVER w
      ) AS mom_latest_stables_onchain_usd,
      SUM(sum_revshare_estimated_eth) OVER w AS all_time_sum_revshare_estimated_eth,
      SUM(sum_revshare_estimated_usd) OVER w AS all_time_sum_revshare_estimated_usd,
      SUM(sum_revshare_actual_eth) OVER w AS all_time_sum_revshare_actual_eth,
      SUM(sum_revshare_actual_usd) OVER w AS all_time_sum_revshare_actual_usd,
    FROM tot_superchain ts
    INNER JOIN tot_eco te USING(dt_month)
    WINDOW w AS (ORDER BY dt_month ASC)
    ORDER BY dt_month DESC
  )
  SELECT
    STRUCT(
      ecosystem_pct_of_l2_rev_fees_usd AS pct_of_l2_rev_fees_usd,
      mom_ecosystem_pct_of_l2_rev_fees_usd AS mom_pct_of_l2_rev_fees_usd,
      ecosystem_pct_of_l2_txs AS pct_of_l2_txs,
      mom_ecosystem_pct_of_l2_txs AS mom_pct_of_l2_txs
    ) AS ecosystem,
    STRUCT(
      sum_total_rev_txn_fees_usd_per_day AS total_usd_per_day,
      sum_total_rev_txn_fees_eth_per_day AS total_eth_per_day,
      mom_sum_total_rev_txn_fees_usd_per_day AS mom_usd_per_day,
      mom_sum_total_rev_txn_fees_eth_per_day AS mom_eth_per_day
    ) AS txn_fees,
    STRUCT(
      sum_txs_per_day AS per_day,
      mom_sum_txs_per_day AS mom_per_day
    ) AS txs,
    STRUCT(
      sum_dex_volume_usd_per_day AS usd_per_day,
      mom_sum_dex_volume_usd_per_day AS mom_usd_per_day
    ) AS dex_volume, 
    STRUCT(
      latest_app_tvl_usd AS app_usd,
      mom_latest_app_tvl_usd AS mom_app_usd
    ) AS tvl,
    STRUCT(
      latest_onchain_value_usd AS onchain_usd,
      mom_latest_onchain_value_usd AS mom_onchain_usd
    ) AS aop,
    STRUCT(
      latest_stables_onchain_usd AS onchain_usd,
      mom_latest_stables_onchain_usd AS mom_onchain_usd
    ) AS stables,
    STRUCT(
      all_time_sum_revshare_estimated_eth AS revshare_estimated_eth,
      all_time_sum_revshare_estimated_usd AS revshare_estimated_usd,
      all_time_sum_revshare_actual_eth AS revshare_actual_eth,
      all_time_sum_revshare_actual_usd AS revshare_actual_usd
    ) AS all_time_revenue
  FROM cte
  WHERE dt_month > '2024-06-01'
  QUALIFY ROW_NUMBER() OVER(ORDER BY dt_month DESC) = 1
  ;
CREATE TEMP TABLE tiles_share AS
  WITH grouped AS (
    SELECT
      dt_month,
      agg_display_name,
      SUM(IFNULL(sum_txs_per_day, 0)) AS sum_txs_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_usd_per_day, 0)) AS sum_total_rev_txn_fees_usd_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_eth_per_day, 0)) AS sum_total_rev_txn_fees_eth_per_day,
      SUM(IFNULL(sum_dex_volume_usd_per_day, 0)) AS sum_dex_volume_usd_per_day,
      SUM(IFNULL(latest_app_tvl_usd, 0)) AS latest_app_tvl_usd,
      SUM(IFNULL(latest_onchain_value_usd, 0)) AS latest_onchain_value_usd,
      SUM(IFNULL(latest_stables_onchain_usd, 0)) AS latest_stables_onchain_usd
    FROM base
    GROUP BY 1, 2
  ),
  totals AS (
    SELECT
      dt_month,
      SUM(sum_txs_per_day) AS tot_sum_txs_per_day,
      SUM(sum_total_rev_txn_fees_usd_per_day) AS tot_sum_total_rev_txn_fees_usd_per_day,
      SUM(sum_total_rev_txn_fees_eth_per_day) AS tot_sum_total_rev_txn_fees_eth_per_day,
      SUM(sum_dex_volume_usd_per_day) AS tot_sum_dex_volume_usd_per_day,
      SUM(latest_app_tvl_usd) AS tot_latest_app_tvl_usd,
      SUM(latest_onchain_value_usd) AS tot_latest_onchain_value_usd,
      SUM(latest_stables_onchain_usd) AS tot_latest_stables_onchain_usd
    FROM grouped
    GROUP BY 1
  )
  SELECT
    agg_display_name,
    ARRAY_AGG(
      STRUCT(
        g.dt_month,
        -- g.sum_txs_per_day AS txs_per_day,
        SAFE_DIVIDE(g.sum_txs_per_day, t.tot_sum_txs_per_day) AS txs_per_day_share,
        -- g.sum_total_rev_txn_fees_usd_per_day AS txn_fees_usd_per_day,
        SAFE_DIVIDE(g.sum_total_rev_txn_fees_usd_per_day, t.tot_sum_total_rev_txn_fees_usd_per_day) AS txn_fees_usd_per_day_share,
        -- g.sum_dex_volume_usd_per_day AS dex_volume_usd_per_day,
        SAFE_DIVIDE(g.sum_dex_volume_usd_per_day, t.tot_sum_dex_volume_usd_per_day) AS dex_volume_usd_per_day_share,
        -- g.latest_app_tvl_usd AS app_tvl_usd,
        SAFE_DIVIDE(g.latest_app_tvl_usd, t.tot_latest_app_tvl_usd) AS app_tvl_usd_share,
        -- g.latest_onchain_value_usd AS onchain_value_usd,
        SAFE_DIVIDE(g.latest_onchain_value_usd, t.tot_latest_onchain_value_usd) AS onchain_value_usd_share,
        -- g.latest_stables_onchain_usd AS stables_onchain_usd,
        SAFE_DIVIDE(g.latest_stables_onchain_usd, t.tot_latest_stables_onchain_usd) AS stables_onchain_usd_share
      )
      ORDER BY g.dt_month DESC
    ) AS popout
  FROM grouped g
  JOIN totals t USING (dt_month)
  WHERE g.dt_month >= '2021-01-01'
  GROUP BY 1;

CREATE TEMP TABLE op_l2_chains AS
WITH filtered AS (
  SELECT *
  FROM base
  WHERE agg_display_name = 'Optimism: Superchain'
)
SELECT
  display_name,
  ARRAY_AGG(
    STRUCT(
      dt_month,
      sum_txs_per_day AS txs_per_day,
      sum_total_rev_txn_fees_usd_per_day AS txn_fees_usd_per_day,
      sum_total_rev_txn_fees_eth_per_day AS txn_fees_eth_per_day,
      sum_dex_volume_usd_per_day AS dex_volume_usd_per_day,
      latest_app_tvl_usd AS app_tvl_usd,
      latest_onchain_value_usd AS onchain_value_usd,
      latest_stables_onchain_usd AS stables_onchain_usd
    )
    ORDER BY dt_month DESC
  ) AS popout
FROM filtered
GROUP BY 1
;
CREATE TEMP TABLE op_l2_totals_share AS
  WITH grouped AS (
    SELECT
      dt_month,
      agg_display_name,
      SUM(IFNULL(sum_txs_per_day, 0)) AS sum_txs_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_usd_per_day, 0)) AS sum_total_rev_txn_fees_usd_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_eth_per_day, 0)) AS sum_total_rev_txn_fees_eth_per_day,
      SUM(IFNULL(sum_dex_volume_usd_per_day, 0)) AS sum_dex_volume_usd_per_day,
      SUM(IFNULL(latest_app_tvl_usd, 0)) AS latest_app_tvl_usd,
      SUM(IFNULL(latest_onchain_value_usd, 0)) AS latest_onchain_value_usd,
      SUM(IFNULL(latest_stables_onchain_usd, 0)) AS latest_stables_onchain_usd
    FROM base
    WHERE agg_display_name = 'Optimism: Superchain'
    GROUP BY 1, 2
  ),
  totals AS (
    SELECT
      dt_month,
      SUM(IFNULL(sum_txs_per_day, 0)) AS tot_sum_txs_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_usd_per_day, 0)) AS tot_sum_total_rev_txn_fees_usd_per_day,
      SUM(IFNULL(sum_total_rev_txn_fees_eth_per_day, 0)) AS tot_sum_total_rev_txn_fees_eth_per_day,
      SUM(IFNULL(sum_dex_volume_usd_per_day, 0)) AS tot_sum_dex_volume_usd_per_day,
      SUM(IFNULL(latest_app_tvl_usd, 0)) AS tot_latest_app_tvl_usd,
      SUM(IFNULL(latest_onchain_value_usd, 0)) AS tot_latest_onchain_value_usd,
      SUM(IFNULL(latest_stables_onchain_usd, 0)) AS tot_latest_stables_onchain_usd
    FROM base_all
    GROUP BY 1
  )
  SELECT
    agg_display_name,
    ARRAY_AGG(
      STRUCT(
        g.dt_month,
        SAFE_DIVIDE(g.sum_txs_per_day, t.tot_sum_txs_per_day) AS txs_per_day_share,
        SAFE_DIVIDE(g.sum_total_rev_txn_fees_usd_per_day, t.tot_sum_total_rev_txn_fees_usd_per_day) AS txn_fees_usd_per_day_share,
        SAFE_DIVIDE(g.sum_total_rev_txn_fees_eth_per_day, t.tot_sum_total_rev_txn_fees_eth_per_day) AS txn_fees_eth_per_day_share,
        SAFE_DIVIDE(g.sum_dex_volume_usd_per_day, t.tot_sum_dex_volume_usd_per_day) AS dex_volume_usd_per_day_share,
        SAFE_DIVIDE(g.latest_app_tvl_usd, t.tot_latest_app_tvl_usd) AS app_tvl_usd_share,
        SAFE_DIVIDE(g.latest_onchain_value_usd, t.tot_latest_onchain_value_usd) AS onchain_value_usd_share,
        SAFE_DIVIDE(g.latest_stables_onchain_usd, t.tot_latest_stables_onchain_usd) AS stables_onchain_usd_share
      )
      ORDER BY g.dt_month DESC
    ) AS shares
  FROM grouped g
  JOIN totals t USING (dt_month)
  WHERE g.dt_month >= '2021-01-01'
  GROUP BY 1
  ;
END