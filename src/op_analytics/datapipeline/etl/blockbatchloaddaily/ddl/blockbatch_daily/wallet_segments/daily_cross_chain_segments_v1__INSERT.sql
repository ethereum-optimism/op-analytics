WITH
    DailyAddressStats_7d AS (
        SELECT
            dt, from_address, chain, chain_id, network,
            count_transactions, count_success_transactions, count_distinct_blocks,
            count_distinct_success_blocks, count_distinct_active_hours,
            count_distinct_to_address, count_distinct_success_to_address,
            count_distinct_method_id, active_block_range, active_nonce_range,
            active_time_range, sum_l2_gas_used, sum_success_l2_gas_used,
            sum_l1_gas_used_unified, sum_success_l1_gas_used_unified, sum_tx_fee_native,
            sum_success_tx_fee_native, sum_l2_fee_native, sum_success_l2_fee_native,
            sum_l2_base_fee_native, sum_success_l2_base_fee_native, sum_l2_priority_fee_native,
            sum_success_l2_priority_fee_native, sum_l2_legacy_extra_fee_native,
            sum_success_l2_legacy_extra_fee_native, sum_l1_fee_native, sum_success_l1_fee_native,
            sum_l1_base_fee_native, sum_success_l1_base_fee_native, sum_l1_blob_fee_native,
            sum_success_l1_blob_fee_native, avg_l2_gas_price_gwei, avg_l2_priority_gas_price_gwei,
            avg_l2_legacy_extra_gas_price_gwei, avg_l1_base_gas_price_gwei,
            avg_l1_blob_base_gas_price_gwei, sum_input_zero_bytes, sum_success_input_zero_bytes,
            sum_input_bytes_length, sum_success_input_bytes_length, sum_input_nonzero_bytes,
            sum_success_input_nonzero_bytes, sum_estimated_size, sum_success_estimated_size
        FROM INPUT_CLICKHOUSE('blockbatch_daily/aggtxs/daily_address_summary_v1', 'lookback_days=7')
        WHERE count_transactions > 0
    ),

    Wallet7DayAggregation AS (
        SELECT
            from_address,
            sumMap(map(chain, count_transactions)) AS map_total_native_tx_counts_7d
        FROM DailyAddressStats_7d
        GROUP BY from_address
    ),

    WalletCrossChainSegments_7d AS (
        SELECT
            from_address,
            CAST(length(mapKeys(map_total_native_tx_counts_7d)) AS UInt8) AS count_distinct_chains_7d,
            count_distinct_chains_7d > 1 AS is_multi_chain_7d,
            multiIf(
                count_distinct_chains_7d = 1, 'Single Chain',
                count_distinct_chains_7d = 2, '2 Chains',
                count_distinct_chains_7d BETWEEN 3 AND 5, '3-5 Chains',
                count_distinct_chains_7d >= 6, '6+ Chains',
                'Undefined Category'
            ) AS multi_chain_category
        FROM Wallet7DayAggregation
    ),

    OverallMaxDate AS (
        SELECT max(dt) AS overall_max_dt FROM DailyAddressStats_7d
    ),

    LatestDayFilteredStatsWithSegment AS (
        SELECT
            ccs.multi_chain_category,
            ccs.is_multi_chain_7d,
            ccs.count_distinct_chains_7d,
            das.*
        FROM DailyAddressStats_7d AS das
        INNER JOIN WalletCrossChainSegments_7d AS ccs ON das.from_address = ccs.from_address
        WHERE das.dt = (SELECT overall_max_dt FROM OverallMaxDate)
    )

SELECT
  (SELECT overall_max_dt FROM OverallMaxDate) AS dt,
  chain, chain_id, network,
  if(grouping(multi_chain_category) = 1, 'Baseline', multi_chain_category) AS wallet_cross_chain_segment,

  count(DISTINCT from_address) AS wallet_count,
  avg(count_distinct_chains_7d) AS avg_wallet_total_distinct_chains_7d,
  max(CAST(is_multi_chain_7d AS UInt8)) = 1 AS includes_multi_chain_wallets_7d,

  sum(count_transactions) AS total_tx_count,
  sum(count_success_transactions) AS total_success_tx_count,
  sum(sum_l2_gas_used) AS total_l2_gas_used, sum(sum_success_l2_gas_used) AS total_success_l2_gas_used,
  sum(sum_l1_gas_used_unified) AS total_l1_gas_used, sum(sum_success_l1_gas_used_unified) AS total_success_l1_gas_used,
  sum(sum_tx_fee_native) AS total_tx_fee_native, sum(sum_success_tx_fee_native) AS total_success_tx_fee_native,
  sum(sum_l2_fee_native) AS total_l2_fee_native, sum(sum_success_l2_fee_native) AS total_success_l2_fee_native,
  sum(sum_l2_base_fee_native) AS total_l2_base_fee_native, sum(sum_success_l2_base_fee_native) AS total_success_l2_base_fee_native,
  sum(sum_l2_priority_fee_native) AS total_l2_priority_fee_native, sum(sum_success_l2_priority_fee_native) AS total_success_l2_priority_fee_native,
  sum(sum_l2_legacy_extra_fee_native) AS total_l2_legacy_extra_fee_native, sum(sum_success_l2_legacy_extra_fee_native) AS total_success_l2_legacy_extra_fee_native,
  sum(sum_l1_fee_native) AS total_l1_fee_native, sum(sum_success_l1_fee_native) AS total_success_l1_fee_native,
  sum(sum_l1_base_fee_native) AS total_l1_base_fee_native, sum(sum_success_l1_base_fee_native) AS total_success_l1_base_fee_native,
  sum(sum_l1_blob_fee_native) AS total_l1_blob_fee_native, sum(sum_success_l1_blob_fee_native) AS total_success_l1_blob_fee_native,
  sum(sum_input_zero_bytes) AS total_input_zero_bytes, sum(sum_success_input_zero_bytes) AS total_success_input_zero_bytes,
  sum(sum_input_bytes_length) AS total_input_bytes_length, sum(sum_success_input_bytes_length) AS total_success_input_bytes_length,
  sum(sum_input_nonzero_bytes) AS total_input_nonzero_bytes, sum(sum_success_input_nonzero_bytes) AS total_success_input_nonzero_bytes,
  sum(sum_estimated_size) AS total_estimated_size, sum(sum_success_estimated_size) AS total_success_estimated_size,
  avg(count_transactions) AS avg_tx_count_per_wallet_on_chain, avg(count_success_transactions) AS avg_success_tx_count_per_wallet_on_chain,
  avg(count_distinct_blocks) AS avg_distinct_blocks_per_wallet_on_chain, avg(count_distinct_success_blocks) AS avg_distinct_success_blocks_per_wallet_on_chain,
  avg(count_distinct_active_hours) AS avg_distinct_active_hours_per_wallet_on_chain, avg(count_distinct_to_address) AS avg_distinct_to_address_per_wallet_on_chain,
  avg(count_distinct_success_to_address) AS avg_distinct_success_to_address_per_wallet_on_chain, avg(count_distinct_method_id) AS avg_distinct_method_id_per_wallet_on_chain,
  avg(active_block_range) AS avg_active_block_range_per_wallet_on_chain, avg(active_nonce_range) AS avg_active_nonce_range_per_wallet_on_chain,
  avg(active_time_range) AS avg_active_time_range_sec_per_wallet_on_chain, avg(sum_l2_gas_used) AS avg_l2_gas_used_per_wallet_on_chain,
  avg(sum_success_l2_gas_used) AS avg_success_l2_gas_used_per_wallet_on_chain, avg(sum_l1_gas_used_unified) AS avg_l1_gas_used_per_wallet_on_chain,
  avg(sum_success_l1_gas_used_unified) AS avg_success_l1_gas_used_per_wallet_on_chain, avg(sum_tx_fee_native) AS avg_tx_fee_native_per_wallet_on_chain,
  avg(sum_success_tx_fee_native) AS avg_success_tx_fee_native_per_wallet_on_chain, avg(sum_l2_fee_native) AS avg_l2_fee_native_per_wallet_on_chain,
  avg(sum_success_l2_fee_native) AS avg_success_l2_fee_native_per_wallet_on_chain, avg(sum_l2_base_fee_native) AS avg_l2_base_fee_native_per_wallet_on_chain,
  avg(sum_success_l2_base_fee_native) AS avg_success_l2_base_fee_native_per_wallet_on_chain, avg(sum_l2_priority_fee_native) AS avg_l2_priority_fee_native_per_wallet_on_chain,
  avg(sum_success_l2_priority_fee_native) AS avg_success_l2_priority_fee_native_per_wallet_on_chain, avg(sum_l2_legacy_extra_fee_native) AS avg_l2_legacy_extra_fee_native_per_wallet_on_chain,
  avg(sum_success_l2_legacy_extra_fee_native) AS avg_success_l2_legacy_extra_fee_native_per_wallet_on_chain, avg(sum_l1_fee_native) AS avg_l1_fee_native_per_wallet_on_chain,
  avg(sum_success_l1_fee_native) AS avg_success_l1_fee_native_per_wallet_on_chain, avg(sum_l1_base_fee_native) AS avg_l1_base_fee_native_per_wallet_on_chain,
  avg(sum_success_l1_base_fee_native) AS avg_success_l1_base_fee_native_per_wallet_on_chain, avg(sum_l1_blob_fee_native) AS avg_l1_blob_fee_native_per_wallet_on_chain,
  avg(sum_success_l1_blob_fee_native) AS avg_success_l1_blob_fee_native_per_wallet_on_chain, avg(avg_l2_gas_price_gwei) AS avg_l2_gas_price_gwei_across_wallets_on_chain,
  avg(avg_l2_priority_gas_price_gwei) AS avg_l2_priority_gas_price_gwei_across_wallets_on_chain, avg(avg_l2_legacy_extra_gas_price_gwei) AS avg_l2_legacy_extra_gas_price_gwei_across_wallets_on_chain,
  avg(avg_l1_base_gas_price_gwei) AS avg_l1_base_gas_price_gwei_across_wallets_on_chain, avg(avg_l1_blob_base_gas_price_gwei) AS avg_l1_blob_base_gas_price_gwei_across_wallets_on_chain,
  avg(sum_input_zero_bytes) AS avg_input_zero_bytes_per_wallet_on_chain, avg(sum_success_input_zero_bytes) AS avg_success_input_zero_bytes_per_wallet_on_chain,
  avg(sum_input_bytes_length) AS avg_input_bytes_length_per_wallet_on_chain, avg(sum_success_input_bytes_length) AS avg_success_input_bytes_length_per_wallet_on_chain,
  avg(sum_input_nonzero_bytes) AS avg_input_nonzero_bytes_per_wallet_on_chain, avg(sum_success_input_nonzero_bytes) AS avg_success_input_nonzero_bytes_per_wallet_on_chain,
  avg(sum_estimated_size) AS avg_estimated_size_per_wallet_on_chain, avg(sum_success_estimated_size) AS avg_success_estimated_size_per_wallet_on_chain,
  sum(count_success_transactions) / nullIf(sum(count_transactions), 0) AS overall_success_rate,
  sum(sum_tx_fee_native) / nullIf(sum(count_transactions), 0) AS overall_avg_tx_fee_native_per_tx,
  sum(sum_l2_gas_used) / nullIf(sum(count_transactions), 0) AS overall_avg_l2_gas_per_tx,
  sum(sum_l1_gas_used_unified) / nullIf(sum(count_transactions), 0) AS overall_avg_l1_gas_per_tx

FROM LatestDayFilteredStatsWithSegment
GROUP BY GROUPING SETS (
  (dt, chain, chain_id, network, multi_chain_category),
  (dt, chain, chain_id, network)
)

SETTINGS use_hive_partitioning = 1