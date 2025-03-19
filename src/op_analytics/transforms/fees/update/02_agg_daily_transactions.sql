SELECT
  -- Descriptors
  dt
  , chain
  , chain_id
  , network

  -- Aggregations
  , count_transactions
  , count_success_transactions
  , distinct_blocks

  -- Gas Usage
  , sum_l2_gas_used
  , sum_success_l2_gas_used

  -- Transaction Fees
  , sum_tx_fee_native
  , sum_success_tx_fee_native

  -- L1 Fees
  , sum_l1_fee_native
  , sum_success_l1_fee_native

  -- L2 Fees
  , sum_l2_fee_native
  , sum_success_l2_fee_native

  -- L2 Priority Fees
  , sum_l2_priority_fee_native
  , sum_success_l2_priority_fee_native

  -- L2 Base Fees
  , sum_l2_base_fee_native
  , sum_success_l2_base_fee_native

  -- L2 Legacy Extra Fees
  , sum_l2_legacy_extra_fee_native
  , sum_success_l2_legacy_extra_fee_native

  -- Gas Prices
  , avg_l2_gas_price_gwei
  , avg_l2_priority_gas_price_gwei
  , avg_l2_legacy_extra_gas_price_gwei
  , avg_l1_base_gas_price_gwei
  , avg_l1_blob_base_gas_price_gwei

  -- Transaction Sizes
  , sum_input_bytes_length
  , sum_input_zero_bytes
  , sum_input_nonzero_bytes
  , sum_l1_base_scaled_size
  , sum_l1_blob_scaled_size
  , sum_l1_gas_used_unified
  , sum_estimated_size
FROM transforms_fees.agg_daily_transactions_grouping_sets FINAL
WHERE
  dt = { dtparam: Date }
  AND from_address = '--'
  AND to_address = '--'
  AND method_id = '--'
