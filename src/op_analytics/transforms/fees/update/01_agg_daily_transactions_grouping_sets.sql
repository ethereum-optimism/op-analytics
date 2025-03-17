/**

Aggregates at the transaction level, at the most granular-level (from, to, method).

    "From address" enables filtering by known senders (i.e. bots, relayers).
    "Method ID" enables segmenting and filtering by function called (i.e. swap, transfer, approve, handle userops)
    "To address" is the contract (or address) that the sender (from address) is directly interacting with.

Downstream, we can use contracts tables to filter for transactions to contracts versus others.
*/

WITH aggregates AS (
  SELECT
    -- Descriptors
    dt
    , chain
    , chain_id
    , network
    , from_address
    , to_address
    , method_id

    -- Aggregations
    , COUNT(*) AS count_transactions
    , countIf(success) AS count_success_transactions

    , COUNT(DISTINCT block_number) AS distinct_blocks

    , SUM(l2_gas_used) AS sum_l2_gas_used
    , sumIf(l2_gas_used, success) AS sum_success_l2_gas_used

    , SUM(CAST(tx_fee_native AS Nullable(Float64))) AS sum_tx_fee_native
    , sumIf(CAST(tx_fee_native AS Nullable(Float64)), success) AS sum_success_tx_fee_native

    , SUM(CAST(l1_fee_native AS Nullable(Float64))) AS sum_l1_fee_native
    , sumIf(CAST(l1_fee_native AS Nullable(Float64)), success) AS sum_success_l1_fee_native

    , SUM(CAST(l2_fee_native AS Nullable(Float64))) AS sum_l2_fee_native
    , sumIf(CAST(l2_fee_native AS Nullable(Float64)), success) AS sum_success_l2_fee_native

    , SUM(CAST(l2_priority_fee_native AS Nullable(Float64))) AS sum_l2_priority_fee_native
    , sumIf(CAST(l2_priority_fee_native AS Nullable(Float64)), success) AS sum_success_l2_priority_fee_native

    , SUM(CAST(l2_base_fee_native AS Nullable(Float64))) AS sum_l2_base_fee_native
    , sumIf(CAST(l2_base_fee_native AS Nullable(Float64)), success) AS sum_success_l2_base_fee_native

    , SUM(CAST(l2_legacy_extra_fee_native AS Nullable(Float64))) AS sum_l2_legacy_extra_fee_native
    , sumIf(CAST(l2_legacy_extra_fee_native AS Nullable(Float64)), success) AS sum_success_l2_legacy_extra_fee_native

    -- Transaction-Level Gas Prices
    , SUM(l2_gas_used * CAST(l2_gas_price_gwei AS Nullable(Float64))) / SUM(l2_gas_used) AS avg_l2_gas_price_gwei
    , SUM(l2_gas_used * CAST(l2_priority_gas_price_gwei AS Nullable(Float64))) / SUM(l2_gas_used) AS avg_l2_priority_gas_price_gwei
    , SUM(l2_gas_used * CAST(l2_legacy_extra_gas_price_gwei AS Nullable(Float64))) / SUM(l2_gas_used) AS avg_l2_legacy_extra_gas_price_gwei
    , SUM(l2_gas_used * CAST(l1_base_gas_price_gwei AS Nullable(Float64))) / SUM(l2_gas_used) AS avg_l1_base_gas_price_gwei
    , SUM(l2_gas_used * CAST(l1_blob_base_gas_price_gwei AS Nullable(Float64))) / SUM(l2_gas_used) AS avg_l1_blob_base_gas_price_gwei

    -- Transaction-Level transaction sizes
    , SUM(input_byte_length) AS sum_input_bytes_length
    , SUM(input_zero_bytes) AS sum_input_zero_bytes
    , SUM(input_byte_length - input_zero_bytes) AS sum_input_nonzero_bytes
    , SUM(l1_base_scaled_size) AS sum_l1_base_scaled_size
    , SUM(l1_blob_scaled_size) AS sum_l1_blob_scaled_size
    , SUM(l1_gas_used_unified) AS sum_l1_gas_used_unified
    , SUM(estimated_size) AS sum_estimated_size


  FROM
    blockbatch_gcs.read_date(
      rootpath = 'blockbatch/refined_traces/refined_transactions_fees_v1'
      , chain = '*'
      , dt = { dtparam: Date }
    )
  WHERE NOT is_system_transaction

  GROUP BY
    GROUPING SETS(
          (dt, chain, chain_id, network, from_address, to_address, method_id),
          (dt, chain, chain_id, network, to_address, method_id),
          (dt, chain, chain_id, network, to_address),
          (dt, chain, chain_id, network)
      )
)

SELECT
  -- Descriptors
  dt
  , chain
  , chain_id
  , network

  -- Use the '--' default value to avoid null values on the group by columns
  , ifNull(from_address, '--') AS from_address
  , ifNull(to_address, '--') AS to_address
  , ifNull(method_id, '--') AS method_id

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
FROM aggregates


SETTINGS use_hive_partitioning = 1
