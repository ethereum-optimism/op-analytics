WITH

tx_fees AS (
  SELECT
    dt
    , chain
    , chain_id
    , network
    , from_address
    , to_address
    , accurateCast(success, 'Bool') AS success
    , accurateCast(block_number, 'UInt64') AS block_number
    , accurateCast(nonce, 'UInt64') AS nonce
    , accurateCast(block_timestamp, 'UInt64') AS block_timestamp
    , accurateCast(block_hour, 'DateTime64(6)') AS block_hour
    , accurateCast(method_id, 'FixedString(10)') AS method_id
    , accurateCast(l2_gas_used, 'UInt256') AS l2_gas_used
    , accurateCast(l1_gas_used_unified, 'UInt256') AS l1_gas_used_unified
    , accurateCast(tx_fee_native, 'Float64') AS tx_fee_native
    , accurateCast(l2_fee_native, 'Float64') AS l2_fee_native
    , accurateCast(l2_base_fee_native, 'Float64') AS l2_base_fee_native
    , accurateCast(l2_priority_fee_native, 'Float64') AS l2_priority_fee_native
    , accurateCast(l2_legacy_extra_fee_native, 'Float64') AS l2_legacy_extra_fee_native
    , accurateCast(l1_fee_native, 'Float64') AS l1_fee_native
    , accurateCast(coalesce(l1_base_fee_native, 0), 'Float64') AS l1_base_fee_native
    , accurateCast(l1_blob_fee_native, 'Float64') AS l1_blob_fee_native
    , accurateCast(l2_gas_price_gwei, 'Float64') AS l2_gas_price_gwei
    , accurateCast(l2_priority_gas_price_gwei, 'Float64') AS l2_priority_gas_price_gwei
    , accurateCast(l2_legacy_extra_gas_price_gwei, 'Float64') AS l2_legacy_extra_gas_price_gwei
    , accurateCast(l1_base_gas_price_gwei, 'Nullable(Float64)') AS l1_base_gas_price_gwei
    , accurateCast(l1_blob_base_gas_price_gwei, 'Nullable(Float64)') AS l1_blob_base_gas_price_gwei
    , accurateCast(input_byte_length, 'UInt32') AS input_byte_length
    , accurateCast(input_zero_bytes, 'UInt32') AS input_zero_bytes
    , accurateCast(coalesce(estimated_size, 0), 'UInt64') AS estimated_size
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'blockbatch/refined_traces/refined_transactions_fees_v1'
      , chain = '*'
      , dt = { dtparam: Date }
    )
  WHERE
    NOT is_system_transaction
)


SELECT
  dt
  , chain
  , chain_id
  , network
  , from_address

  -- Aggregates

  , count(*) AS count_transactions
  , countIf(success) AS count_success_transactions

  , count(DISTINCT block_number) AS count_distinct_blocks
  , count(DISTINCT if(success, block_number, NULL)) AS count_distinct_success_blocks

  , min(block_number) AS min_block_number
  , max(block_number) AS max_block_number
  , max(block_number) - min(block_number) + 1 AS active_block_range

  , min(nonce) AS min_nonce
  , max(nonce) AS max_nonce
  , max(nonce) - min(nonce) + 1 AS active_nonce_range

  , min(block_timestamp) AS min_block_timestamp
  , max(block_timestamp) AS max_block_timestamp
  , max(block_timestamp) - min(block_timestamp) AS active_time_range
  , count(DISTINCT block_hour) AS count_distinct_active_hours

  , count(DISTINCT to_address) AS count_distinct_to_address
  , count(DISTINCT if(success, to_address, NULL)) AS count_distinct_success_to_address
  , count(DISTINCT method_id) AS count_distinct_method_id

  , sum(l2_gas_used) AS sum_l2_gas_used
  , sumIf(l2_gas_used, success) AS sum_success_l2_gas_used

  , sum(l1_gas_used_unified) AS sum_l1_gas_used_unified
  , sumIf(l1_gas_used_unified, success) AS sum_success_l1_gas_used_unified

  , sum(tx_fee_native) AS sum_tx_fee_native
  , sumIf(tx_fee_native, success) AS sum_success_tx_fee_native

  -- L2 Fee and breakdown into BASE + PRIORITY
  , sum(l2_fee_native) AS sum_l2_fee_native
  , sumIf(l2_fee_native, success) AS sum_success_l2_fee_native

  , sum(l2_base_fee_native) AS sum_l2_base_fee_native
  , sumIf(l2_base_fee_native, success) AS sum_success_l2_base_fee_native

  , sum(l2_priority_fee_native) AS sum_l2_priority_fee_native
  , sumIf(l2_priority_fee_native, success) AS sum_success_l2_priority_fee_native

  , sum(l2_legacy_extra_fee_native) AS sum_l2_legacy_extra_fee_native
  , sumIf(l2_legacy_extra_fee_native, success) AS sum_success_l2_legacy_extra_fee_native

  -- L1 Fee and breakdown into BASE + BLOB
  , sum(l1_fee_native) AS sum_l1_fee_native
  , sumIf(l1_fee_native, success) AS sum_success_l1_fee_native

  , sum(l1_base_fee_native) AS sum_l1_base_fee_native
  , sumIf(l1_base_fee_native, success) AS sum_success_l1_base_fee_native

  , sum(l1_blob_fee_native) AS sum_l1_blob_fee_native
  , sumIf(l1_blob_fee_native, success) AS sum_success_l1_blob_fee_native

  -- Transaction-Level Gas Prices
  , avgWeighted(l2_gas_price_gwei, l2_gas_used) AS avg_l2_gas_price_gwei
  , avgWeighted(l2_priority_gas_price_gwei, l2_gas_used) AS avg_l2_priority_gas_price_gwei
  , avgWeighted(l2_legacy_extra_gas_price_gwei, l2_gas_used) AS avg_l2_legacy_extra_gas_price_gwei
  , avgWeighted(l1_base_gas_price_gwei, l2_gas_used) AS avg_l1_base_gas_price_gwei
  , avgWeighted(l1_blob_base_gas_price_gwei, l2_gas_used) AS avg_l1_blob_base_gas_price_gwei

  -- Data Processed
  , sum(input_zero_bytes) AS sum_input_zero_bytes
  , sumIf(input_zero_bytes, success) AS sum_success_input_zero_bytes

  , sum(input_byte_length) AS sum_input_bytes_length
  , sumIf(input_byte_length, success) AS sum_success_input_bytes_length

  , sum(input_byte_length - input_zero_bytes) AS sum_input_nonzero_bytes
  , sumIf(input_byte_length - input_zero_bytes, success) AS sum_success_input_nonzero_bytes

  , sum(estimated_size) AS sum_estimated_size
  , sumIf(estimated_size, success) AS sum_success_estimated_size

FROM tx_fees

GROUP BY
  dt
  , chain
  , chain_id
  , network
  , from_address

SETTINGS use_hive_partitioning = 1