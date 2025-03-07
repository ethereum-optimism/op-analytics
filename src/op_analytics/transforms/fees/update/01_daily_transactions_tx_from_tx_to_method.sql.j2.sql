/**

Aggregates at the transaction level, at the most granular-level (from, to, method).

    "From address" enables filtering by known senders (i.e. bots, relayers).
    "Method ID" enables segmenting and filtering by function called (i.e. swap, transfer, approve, handle userops)
    "To address" is the contract (or address) that the sender (from address) is directly interacting with.

Downstream, we can use contracts tables to filter for transactions to contracts versus others.
*/

WITH

refined_transactions AS (
  SELECT *
  FROM blockbatch_gcs.read_date(
    rootpath = 'blockbatch/refined_traces/refined_transactions_fees_v1'
    , chain = 'base'
    , dt = '2025-02-06'
  )
)

SELECT
  -- Descriptors
  dt
  , chain
  , network
  , chain_id
  , from_address
  , to_address
  , method_id

  -- Aggregations
  , COUNT(*) AS count_transactions
  , countIf(success) AS count_success_transactions

  , COUNT(DISTINCT block_number) AS distinct_blocks

  , SUM(l2_gas_used) AS sum_l2_gas_used
  , sumIf(l2_gas_used, success) AS sum_success_l2_gas_used

  , SUM(tx_fee_native) AS sum_tx_fee_native
  , sumIf(tx_fee_native, success) AS sum_success_tx_fee_native

  , SUM(l1_fee_native) AS sum_l1_fee_native
  , sumIf(l1_fee_native, success) AS sum_success_l1_fee_native

  , SUM(l2_fee_native) AS sum_l2_fee_native
  , sumIf(l2_fee_native, success) AS sum_success_l2_fee_native

  , SUM(l2_priority_fee_native) AS sum_l2_priority_fee_native
  , sumIf(l2_priority_fee_native, success) AS sum_success_l2_priority_fee_native

  , SUM(l2_base_fee_native) AS sum_l2_base_fee_native
  , sumIf(l2_base_fee_native, success) AS sum_success_l2_base_fee_native

  , SUM(l2_legacy_extra_fee_native) AS sum_l2_legacy_extra_fee_native
  , sumIf(l2_legacy_extra_fee_native, success) AS sum_success_l2_legacy_extra_fee_native

  -- Transaction-Level Gas Prices
  , SUM(l2_gas_used * l2_gas_price_gwei) / SUM(l2_gas_used) AS avg_l2_gas_price_gwei
  , SUM(l2_gas_used * l2_priority_gas_price_gwei) / SUM(l2_gas_used) AS avg_l2_priority_gas_price_gwei
  , SUM(l2_gas_used * l2_legacy_extra_gas_price_gwei) / SUM(l2_gas_used) AS avg_l2_legacy_extra_gas_price_gwei
  , SUM(l2_gas_used * l1_base_gas_price_gwei) / SUM(l2_gas_used) AS avg_l1_base_gas_price_gwei
  , SUM(l2_gas_used * l1_blob_base_gas_price_gwei) / SUM(l2_gas_used) AS avg_l1_blob_base_gas_price_gwei

  -- Transaction-Level transaction sizes
  , SUM(input_byte_length) AS sum_input_bytes_length
  , SUM(input_zero_bytes) AS sum_input_zero_bytes
  , SUM(input_byte_length - input_zero_bytes) AS sum_input_nonzero_bytes
  , SUM(l1_base_scaled_size) AS sum_l1_base_scaled_size
  , SUM(l1_blob_scaled_size) AS sum_l1_blob_scaled_size
  , SUM(l1_gas_used_unified) AS sum_l1_gas_used_unified
  , SUM(estimated_size) AS sum_estimated_size


FROM refined_transactions
WHERE NOT is_system_transaction

GROUP BY
  dt, chain, network, chain_id, from_address, to_address, method_id

SETTINGS use_hive_partitioning = 1


