WITH
DailyWalletActivity AS (

  SELECT
    dt, chain, chain_id, network, from_address,
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
  FROM INPUT_CLICKHOUSE('blockbatch_daily/aggtxs/daily_address_summary_v1')
  WHERE
    1=1
    AND count_transactions > 0
),

DailyWalletCohorts AS (
  -- Assigns segments and calculates per-wallet rates/averages
  SELECT
    dwa.*, -- Select all columns from DailyWalletActivity
    -- Define segments based on daily transaction count
    multiIf(
      dwa.count_transactions >= 501, '>500_tx_per_day',    -- Tier5: Automated (High Freq)
      dwa.count_transactions >= 51,  '51-500_tx_per_day',  -- Tier4: Automated (Low Freq)
      dwa.count_transactions >= 16,  '16-50_tx_per_day',   -- Tier3: Power users
      dwa.count_transactions >= 2,   '2-15_tx_per_day',    -- Tier2: Organic activity
      dwa.count_transactions = 1,    '1_tx_per_day',       -- Tier1: Organic activity (Single)
      'undefined' -- Fallback (shouldn't be hit due to WHERE clause above)
    ) AS segment,
    dwa.count_success_transactions / nullIf(dwa.count_transactions, 0) AS wallet_success_rate,
    dwa.sum_tx_fee_native / nullIf(dwa.count_transactions, 0) AS wallet_avg_fee_per_tx,
    dwa.sum_l2_gas_used / nullIf(dwa.count_transactions, 0) AS wallet_avg_l2_gas_per_tx,
    dwa.sum_l1_gas_used_unified / nullIf(dwa.count_transactions, 0) AS wallet_avg_l1_gas_per_tx
  FROM DailyWalletActivity AS dwa
)

-- Final Aggregation: Group by segment (and overall baseline)
SELECT
  dt, chain, chain_id, network,
  -- Use 'Baseline' for the overall aggregation row, otherwise use the segment name
  if(grouping(segment) = 1, 'Baseline', segment) AS wallet_segment,

  -- === Wallet Count ===
  count(DISTINCT from_address) AS wallet_count,

  -- === TOTAL Sums per segment ===
  sum(count_transactions) AS total_tx_count,
  sum(count_success_transactions) AS total_success_tx_count,
  sum(sum_l2_gas_used) AS total_l2_gas_used,
  sum(sum_success_l2_gas_used) AS total_success_l2_gas_used,
  sum(sum_l1_gas_used_unified) AS total_l1_gas_used,
  sum(sum_success_l1_gas_used_unified) AS total_success_l1_gas_used,
  sum(sum_tx_fee_native) AS total_tx_fee_native,
  sum(sum_success_tx_fee_native) AS total_success_tx_fee_native,
  sum(sum_l2_fee_native) AS total_l2_fee_native,
  sum(sum_success_l2_fee_native) AS total_success_l2_fee_native,
  sum(sum_l2_base_fee_native) AS total_l2_base_fee_native,
  sum(sum_success_l2_base_fee_native) AS total_success_l2_base_fee_native,
  sum(sum_l2_priority_fee_native) AS total_l2_priority_fee_native,
  sum(sum_success_l2_priority_fee_native) AS total_success_l2_priority_fee_native,
  sum(sum_l2_legacy_extra_fee_native) AS total_l2_legacy_extra_fee_native,
  sum(sum_success_l2_legacy_extra_fee_native) AS total_success_l2_legacy_extra_fee_native,
  sum(sum_l1_fee_native) AS total_l1_fee_native,
  sum(sum_success_l1_fee_native) AS total_success_l1_fee_native,
  sum(sum_l1_base_fee_native) AS total_l1_base_fee_native,
  sum(sum_success_l1_base_fee_native) AS total_success_l1_base_fee_native,
  sum(sum_l1_blob_fee_native) AS total_l1_blob_fee_native,
  sum(sum_success_l1_blob_fee_native) AS total_success_l1_blob_fee_native,
  sum(sum_input_zero_bytes) AS total_input_zero_bytes,
  sum(sum_success_input_zero_bytes) AS total_success_input_zero_bytes,
  sum(sum_input_bytes_length) AS total_input_bytes_length,
  sum(sum_success_input_bytes_length) AS total_success_input_bytes_length,
  sum(sum_input_nonzero_bytes) AS total_input_nonzero_bytes,
  sum(sum_success_input_nonzero_bytes) AS total_success_input_nonzero_bytes,
  sum(sum_estimated_size) AS total_estimated_size,
  sum(sum_success_estimated_size) AS total_success_estimated_size,

  -- === AVERAGES PER WALLET (Simple average of daily per-wallet metrics) ===
  -- Base Metrics
  avg(count_transactions) AS avg_tx_count_per_wallet,
  avg(count_success_transactions) AS avg_success_tx_count_per_wallet,
  -- Distinct Counts
  avg(count_distinct_blocks) AS avg_distinct_blocks_per_wallet,
  avg(count_distinct_success_blocks) AS avg_distinct_success_blocks_per_wallet,
  avg(count_distinct_active_hours) AS avg_distinct_active_hours_per_wallet,
  avg(count_distinct_to_address) AS avg_distinct_to_address_per_wallet,
  avg(count_distinct_success_to_address) AS avg_distinct_success_to_address_per_wallet,
  avg(count_distinct_method_id) AS avg_distinct_method_id_per_wallet,
  -- Ranges
  avg(active_block_range) AS avg_active_block_range_per_wallet,
  avg(active_nonce_range) AS avg_active_nonce_range_per_wallet,
  avg(active_time_range) AS avg_active_time_range_sec_per_wallet,
  -- Gas Usage
  avg(sum_l2_gas_used) AS avg_l2_gas_used_per_wallet,
  avg(sum_success_l2_gas_used) AS avg_success_l2_gas_used_per_wallet,
  avg(sum_l1_gas_used_unified) AS avg_l1_gas_used_per_wallet,
  avg(sum_success_l1_gas_used_unified) AS avg_success_l1_gas_used_per_wallet,
  -- Fees
  avg(sum_tx_fee_native) AS avg_tx_fee_native_per_wallet,
  avg(sum_success_tx_fee_native) AS avg_success_tx_fee_native_per_wallet,
  avg(sum_l2_fee_native) AS avg_l2_fee_native_per_wallet,
  avg(sum_success_l2_fee_native) AS avg_success_l2_fee_native_per_wallet,
  avg(sum_l2_base_fee_native) AS avg_l2_base_fee_native_per_wallet,
  avg(sum_success_l2_base_fee_native) AS avg_success_l2_base_fee_native_per_wallet,
  avg(sum_l2_priority_fee_native) AS avg_l2_priority_fee_native_per_wallet,
  avg(sum_success_l2_priority_fee_native) AS avg_success_l2_priority_fee_native_per_wallet,
  avg(sum_l2_legacy_extra_fee_native) AS avg_l2_legacy_extra_fee_native_per_wallet,
  avg(sum_success_l2_legacy_extra_fee_native) AS avg_success_l2_legacy_extra_fee_native_per_wallet,
  avg(sum_l1_fee_native) AS avg_l1_fee_native_per_wallet,
  avg(sum_success_l1_fee_native) AS avg_success_l1_fee_native_per_wallet,
  avg(sum_l1_base_fee_native) AS avg_l1_base_fee_native_per_wallet,
  avg(sum_success_l1_base_fee_native) AS avg_success_l1_base_fee_native_per_wallet,
  avg(sum_l1_blob_fee_native) AS avg_l1_blob_fee_native_per_wallet,
  avg(sum_success_l1_blob_fee_native) AS avg_success_l1_blob_fee_native_per_wallet,
  -- Gas Prices
  avg(avg_l2_gas_price_gwei) AS avg_l2_gas_price_gwei_across_wallets,
  avg(avg_l2_priority_gas_price_gwei) AS avg_l2_priority_gas_price_gwei_across_wallets,
  avg(avg_l2_legacy_extra_gas_price_gwei) AS avg_l2_legacy_extra_gas_price_gwei_across_wallets,
  avg(avg_l1_base_gas_price_gwei) AS avg_l1_base_gas_price_gwei_across_wallets,
  avg(avg_l1_blob_base_gas_price_gwei) AS avg_l1_blob_base_gas_price_gwei_across_wallets,
  -- Input Data
  avg(sum_input_zero_bytes) AS avg_input_zero_bytes_per_wallet,
  avg(sum_success_input_zero_bytes) AS avg_success_input_zero_bytes_per_wallet,
  avg(sum_input_bytes_length) AS avg_input_bytes_length_per_wallet,
  avg(sum_success_input_bytes_length) AS avg_success_input_bytes_length_per_wallet,
  avg(sum_input_nonzero_bytes) AS avg_input_nonzero_bytes_per_wallet,
  avg(sum_success_input_nonzero_bytes) AS avg_success_input_nonzero_bytes_per_wallet,
  -- Estimated Size
  avg(sum_estimated_size) AS avg_estimated_size_per_wallet,
  avg(sum_success_estimated_size) AS avg_success_estimated_size_per_wallet,

  -- === AVERAGES PER TRANSACTION ===
  avgWeighted(count_success_transactions, count_transactions) AS overall_success_rate,
  avgWeighted(sum_tx_fee_native, count_transactions) AS overall_avg_tx_fee_native_per_tx,
  avgWeighted(sum_l2_gas_used, count_transactions) AS overall_avg_l2_gas_per_tx,
  avgWeighted(sum_l1_gas_used_unified, count_transactions) AS overall_avg_l1_gas_per_tx,

  -- === AVERAGES PER WALLET ===
  avg(wallet_success_rate) AS avg_wallet_success_rate,
  avgWeighted(wallet_avg_fee_per_tx, count_transactions) AS avg_wallet_avg_fee_per_tx,
  avgWeighted(wallet_avg_l2_gas_per_tx, count_transactions) AS avg_wallet_avg_l2_gas_per_tx,
  avgWeighted(wallet_avg_l1_gas_per_tx, count_transactions) AS avg_wallet_avg_l1_gas_per_tx

FROM DailyWalletCohorts
-- Group by date, chain, network, and segment, AND also group by just date, chain, network (for baseline)
GROUP BY GROUPING SETS (
  (dt, chain, chain_id, network, segment),
  (dt, chain, chain_id, network)
)
-- Order results for readability
ORDER BY
  dt ASC, chain ASC, network ASC,
  multiIf(
    wallet_segment = '>500_tx_per_day', 1,
    wallet_segment = '51-500_tx_per_day', 2,
    wallet_segment = '16-50_tx_per_day', 3,
    wallet_segment = '2-15_tx_per_day', 4,
    wallet_segment = '1_tx_per_day', 5,
    wallet_segment = 'Baseline', 6,
    99 -- Fallback for any unexpected segment names
  ) ASC,
  chain_id ASC

SETTINGS use_hive_partitioning = 1