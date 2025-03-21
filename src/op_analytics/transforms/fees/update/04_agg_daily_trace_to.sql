-- Aggreagte traces by day and the contract called (to address)
-- This will be used as the final table for dashboards that show the most used contracts in aggregate

-- Get Unique contracts that call this contract, since we lose this granularity at the to_hash level


WITH

trace_fees AS (
  SELECT
    dt
    , chain
    , chain_id
    , network
    , trace_to_address

    -- Transaction level
    , accurateCast(tx_success, 'Bool') AS tx_success
    , accurateCast(block_number, 'UInt64') AS block_number
    , accurateCast(tx_from_address, 'String') AS tx_from_address
    , accurateCast(tx_to_address, 'String') AS tx_to_address
    , accurateCast(tx_method_id, 'String') AS tx_method_id
    , accurateCast(tx_l2_gas_used, 'UInt256') AS tx_l2_gas_used
    , accurateCast(tx_fee_native, 'Float64') AS tx_fee_native

    , accurateCast(tx_l1_fee_native, 'Float64') AS tx_l1_fee_native
    , accurateCast(tx_l2_fee_native, 'Float64') AS tx_l2_fee_native
    , accurateCast(tx_l2_priority_fee_native, 'Float64') AS tx_l2_priority_fee_native
    , accurateCast(tx_l2_base_fee_native, 'Float64') AS tx_l2_base_fee_native
    , accurateCast(tx_l2_legacy_extra_fee_native, 'Float64') AS tx_l2_legacy_extra_fee_native

    , accurateCast(tx_l2_gas_price_gwei, 'Float64') AS tx_l2_gas_price_gwei
    , accurateCast(tx_l2_priority_gas_price_gwei, 'Float64') AS tx_l2_priority_gas_price_gwei
    , accurateCast(tx_l2_legacy_extra_gas_price_gwei, 'Float64') AS tx_l2_legacy_extra_gas_price_gwei
    , accurateCast(tx_l1_base_gas_price_gwei, 'Float64') AS tx_l1_base_gas_price_gwei
    , accurateCast(tx_l1_blob_base_gas_price_gwei, 'Float64') AS tx_l1_blob_base_gas_price_gwei

    , accurateCast(tx_input_byte_length, 'UInt32') AS tx_input_byte_length
    , accurateCast(tx_input_zero_bytes, 'UInt32') AS tx_input_zero_bytes
    , accurateCast(tx_input_nonzero_bytes, 'UInt32') AS tx_input_nonzero_bytes

    , accurateCast(tx_l1_gas_used_unified, 'UInt64') AS tx_l1_gas_used_unified
    , accurateCast(tx_l1_base_scaled_size, 'Float64') AS tx_l1_base_scaled_size
    , accurateCast(tx_l1_blob_scaled_size, 'Float64') AS tx_l1_blob_scaled_size
    , accurateCast(tx_estimated_size, 'UInt64') AS tx_estimated_size

    -- Trace level
    , accurateCast(sum_trace_gas_used, 'Float64') AS sum_trace_gas_used
    , accurateCast(sum_trace_gas_used_minus_subtraces, 'Float64') AS sum_trace_gas_used_minus_subtraces

    , accurateCast(sum_tx_l2_gas_used_amortized_by_call, 'Float64') AS sum_tx_l2_gas_used_amortized_by_call
    , accurateCast(sum_tx_l1_gas_used_unified_amortized_by_call, 'Float64') AS sum_tx_l1_gas_used_unified_amortized_by_call
    , accurateCast(sum_tx_l1_base_scaled_size_amortized_by_call, 'Float64') AS sum_tx_l1_base_scaled_size_amortized_by_call
    , accurateCast(sum_tx_l1_blob_scaled_size_amortized_by_call, 'Float64') AS sum_tx_l1_blob_scaled_size_amortized_by_call
    , accurateCast(sum_tx_estimated_size_amortized_by_call, 'Float64') AS sum_tx_estimated_size_amortized_by_call

    , accurateCast(sum_tx_l2_fee_native_minus_subtraces, 'Float64') AS sum_tx_l2_fee_native_minus_subtraces
    , accurateCast(sum_tx_l2_base_fee_native_minus_subtraces, 'Float64') AS sum_tx_l2_base_fee_native_minus_subtraces
    , accurateCast(sum_tx_l2_priority_fee_native_minus_subtraces, 'Float64') AS sum_tx_l2_priority_fee_native_minus_subtraces
    , accurateCast(sum_tx_l2_legacy_base_fee_native_minus_subtraces, 'Float64') AS sum_tx_l2_legacy_base_fee_native_minus_subtraces

    , accurateCast(sum_tx_fee_native_amortized_by_call, 'Float64') AS sum_tx_fee_native_amortized_by_call
    , accurateCast(sum_tx_l2_fee_native_amortized_by_call, 'Float64') AS sum_tx_l2_fee_native_amortized_by_call
    , accurateCast(sum_tx_l1_fee_native_amortized_by_call, 'Float64') AS sum_tx_l1_fee_native_amortized_by_call
    , accurateCast(sum_tx_l2_base_fee_native_amortized_by_call, 'Float64') AS sum_tx_l2_base_fee_native_amortized_by_call
    , accurateCast(sum_tx_l2_priority_fee_native_amortized_by_call, 'Float64') AS sum_tx_l2_priority_fee_native_amortized_by_call
    , accurateCast(sum_tx_fee_native_l1_amortized_l2_minus_subtraces, 'Float64') AS sum_tx_fee_native_l1_amortized_l2_minus_subtraces

    -- Calls
    , cast(count_top_level_calls, 'UInt32') AS count_top_level_calls
    , cast(count_internal_calls_all_types, 'UInt32') AS count_internal_calls_all_types
    , cast(count_internal_calls_all_types_trace_success, 'UInt32') AS count_internal_calls_all_types_trace_success
    , cast(count_internal_calls_static_type, 'UInt32') AS count_internal_calls_static_type
    , cast(count_internal_calls_static_type_trace_success, 'UInt32') AS count_internal_calls_static_type_trace_success
    , cast(count_internal_calls_delegate_type, 'UInt32') AS count_internal_calls_delegate_type
    , cast(count_internal_calls_delegate_type_trace_success, 'UInt32') AS count_internal_calls_delegate_type_trace_success
    , cast(count_internal_calls_call_type, 'UInt32') AS count_internal_calls_call_type
    , cast(count_internal_calls_call_type_trace_success, 'UInt32') AS count_internal_calls_call_type_trace_success

    , cast(count_traces_type_all_types, 'UInt32') AS count_traces_type_all_types
    , cast(count_traces_trace_type_call, 'UInt32') AS count_traces_trace_type_call
    , cast(count_traces_trace_type_suicide, 'UInt32') AS count_traces_trace_type_suicide
    , cast(count_traces_trace_type_create, 'UInt32') AS count_traces_trace_type_create
    , cast(count_traces_trace_type_create2, 'UInt32') AS count_traces_trace_type_create2
    , cast(count_traces_trace_type_create_any, 'UInt32') AS count_traces_trace_type_create_any

    , cast(is_transaction_with_internal_call_type_call, 'Bool') AS is_transaction_with_internal_call_type_call
    , cast(is_transaction_with_internal_call_type_call_or_delegate, 'Bool') AS is_transaction_with_internal_call_type_call_or_delegate

  FROM
    blockbatch_gcs.read_date(
      rootpath = 'blockbatch/aggregated_traces/agg_traces_tr_to_hash_v1'
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
  , trace_to_address

  -- Aggregate transaction metrics, in transactions when a given contract was called
  /*
    Note: We can NOT sum these up at the project level, since a project may have
    many of its contracts called in a single transaction.

    For these use cases (i.e. project impact metrics), we can aggregate `daily_trace_calls_agg_to_hash`
    at the project name level, only after all trace_to_address entries are mapped to their respective
    project name (i.e. a transaction may call 10 contracts, but this may only represent 3 projects).
  */
  -- Attributes
  , COUNT(*) AS count_transactions_called
  , countIf(tx_success) AS count_success_transactions_called

  , COUNT(DISTINCT block_number) AS count_distinct_blocks_called
  , COUNT(DISTINCT tx_from_address) AS count_distinct_tx_from_addresses
  , COUNT(DISTINCT tx_to_address) AS count_distinct_tx_to_addresses
  , COUNT(DISTINCT tx_method_id) AS count_distinct_tx_method_ids

  -- Transaction-Level gas & fees
  -- QUESTION FOR MICHAEL: Would summing this be double counting?
  , SUM(tx_l2_gas_used) AS sum_tx_l2_gas_used
  , sumIf(tx_l2_gas_used, tx_success) AS sum_tx_success_tx_l2_gas_used

  , SUM(tx_fee_native) AS sum_tx_fee_native
  , sumIf(tx_fee_native, tx_success) AS sum_tx_success_tx_fee_native

  , SUM(tx_l1_fee_native) AS sum_tx_l1_fee_native
  , sumIf(tx_l1_fee_native, tx_success) AS sum_tx_success_tx_l1_fee_native

  , SUM(tx_l2_fee_native) AS sum_tx_l2_fee_native
  , sumIf(tx_l2_fee_native, tx_success) AS sum_tx_success_tx_l2_fee_native

  , SUM(tx_l2_priority_fee_native) AS sum_tx_l2_priority_fee_native
  , sumIf(tx_l2_priority_fee_native, tx_success) AS sum_tx_success_tx_l2_priority_fee_native

  , SUM(tx_l2_base_fee_native) AS sum_tx_l2_base_fee_native
  , sumIf(tx_l2_base_fee_native, tx_success) AS sum_tx_success_tx_l2_base_fee_native

  , SUM(tx_l2_legacy_extra_fee_native) AS sum_tx_l2_legacy_extra_fee_native
  , sumIf(tx_l2_legacy_extra_fee_native, tx_success) AS sum_tx_success_tx_l2_legacy_extra_fee_native

  -- Transaction-Level Gas Prices
  , avgWeighted(tx_l2_gas_price_gwei, tx_l2_gas_used) AS avg_tx_l2_gas_price_gwei_called
  , avgWeighted(tx_l2_priority_gas_price_gwei, tx_l2_gas_used) AS avg_tx_l2_priority_gas_price_gwei_called
  , avgWeighted(tx_l2_legacy_extra_gas_price_gwei, tx_l2_gas_used) AS avg_tx_l2_legacy_extra_gas_price_gwei_called
  , avgWeighted(tx_l1_base_gas_price_gwei, tx_l2_gas_used) AS avg_tx_l1_base_gas_price_gwei_called
  , avgWeighted(tx_l1_blob_base_gas_price_gwei, tx_l2_gas_used) AS avg_tx_l1_blob_base_gas_price_gwei_called 


  -- Transaction-Level transaction sizes
  , SUM(tx_input_byte_length) AS sum_tx_input_byte_length_called
  , SUM(tx_input_zero_bytes) AS sum_tx_input_zero_bytes_called
  , SUM(tx_input_nonzero_bytes) AS sum_tx_input_nonzero_bytes_called
  , SUM(tx_l1_base_scaled_size) AS sum_tx_l1_base_scaled_size_called
  , SUM(tx_l1_blob_scaled_size) AS sum_tx_l1_blob_scaled_size_called
  , SUM(tx_l1_gas_used_unified) AS sum_tx_l1_gas_used_unified_called
  , SUM(tx_estimated_size) AS sum_tx_estimated_size_called

  -- Sum Internal Trace Gas
  /*
    Note: As opposed to transaction-level equivalent metrics, these CAN be summed up at a project level.
    These methodologies were designed to distribute gas usage and fees across each call, so that they
    still represent a "part of a whole," whereas transaction-level metrics do not.
  */

  , SUM(sum_trace_gas_used) AS sum_trace_gas_used
  , SUM(sum_trace_gas_used) FILTER (WHERE tx_success) AS sum_trace_gas_used_tx_success

  , SUM(sum_trace_gas_used_minus_subtraces) AS sum_trace_gas_used_minus_subtraces
  , SUM(sum_trace_gas_used_minus_subtraces) FILTER (WHERE tx_success) AS sum_trace_gas_used_minus_subtraces_tx_success


  , SUM(sum_tx_l2_gas_used_amortized_by_call) AS sum_tx_l2_gas_used_amortized_by_call
  , SUM(sum_tx_l2_gas_used_amortized_by_call) FILTER (WHERE tx_success) AS sum_tx_l2_gas_used_amortized_by_call_tx_success

  , SUM(sum_tx_l1_gas_used_unified_amortized_by_call) AS sum_tx_l1_gas_used_unified_amortized_by_call
  , SUM(sum_tx_l1_base_scaled_size_amortized_by_call) AS sum_tx_l1_base_scaled_size_amortized_by_call
  , SUM(sum_tx_l1_blob_scaled_size_amortized_by_call) AS sum_tx_l1_blob_scaled_size_amortized_by_call
  , SUM(sum_tx_estimated_size_amortized_by_call) AS sum_tx_estimated_size_amortized_by_call

  , SUM(sum_tx_l2_fee_native_minus_subtraces) AS sum_tx_l2_fee_native_minus_subtraces
  , SUM(sum_tx_l2_fee_native_minus_subtraces) FILTER (WHERE tx_success) AS sum_tx_l2_fee_native_minus_subtraces_tx_success

  , SUM(sum_tx_l2_base_fee_native_minus_subtraces) AS sum_tx_l2_base_fee_native_minus_subtraces
  , SUM(sum_tx_l2_priority_fee_native_minus_subtraces) AS sum_tx_l2_priority_fee_native_minus_subtraces
  , SUM(sum_tx_l2_legacy_base_fee_native_minus_subtraces) AS sum_tx_l2_legacy_base_fee_native_minus_subtraces

  , SUM(sum_tx_fee_native_amortized_by_call) AS sum_tx_fee_native_amortized_by_call
  , SUM(sum_tx_fee_native_amortized_by_call) FILTER (WHERE tx_success) AS sum_tx_fee_native_amortized_by_call_tx_success

  , SUM(sum_tx_l2_fee_native_amortized_by_call) AS sum_tx_l2_fee_native_amortized_by_call
  , SUM(sum_tx_l2_fee_native_amortized_by_call) FILTER (WHERE tx_success) AS sum_tx_l2_fee_native_amortized_by_call_tx_success

  , SUM(sum_tx_l1_fee_native_amortized_by_call) AS sum_tx_l1_fee_native_amortized_by_call
  , SUM(sum_tx_l1_fee_native_amortized_by_call) FILTER (WHERE tx_success) AS sum_tx_l1_fee_native_amortized_by_call_tx_success

  , SUM(sum_tx_l2_base_fee_native_amortized_by_call) AS sum_tx_l2_base_fee_native_amortized_by_call
  , SUM(sum_tx_l2_priority_fee_native_amortized_by_call) AS sum_tx_l2_priority_fee_native_amortized_by_call

  , SUM(sum_tx_fee_native_l1_amortized_l2_minus_subtraces) AS sum_tx_fee_native_l1_amortized_l2_minus_subtraces

  -- Calls
  , SUM(count_top_level_calls) AS count_top_level_calls
  --count non-null trace addresses, null is the transaction-level call.
  , SUM(count_internal_calls_all_types) AS count_internal_calls_all_types
  , SUM(count_internal_calls_all_types_trace_success) AS count_internal_calls_all_types_trace_success
  --static calls only read state, and can not write
  , SUM(count_internal_calls_static_type) AS count_internal_calls_static_type
  , SUM(count_internal_calls_static_type_trace_success) AS count_internal_calls_static_type_trace_success
  --delegate calls call a function on another contract
  , SUM(count_internal_calls_delegate_type) AS count_internal_calls_delegate_type
  , SUM(count_internal_calls_delegate_type_trace_success) AS count_internal_calls_delegate_type_trace_success
  --normal function calls
  , SUM(count_internal_calls_call_type) AS count_internal_calls_call_type
  , SUM(count_internal_calls_call_type_trace_success) AS count_internal_calls_call_type_trace_success

  -- Experimental
  , COUNT(*) FILTER (WHERE is_transaction_with_internal_call_type_call) AS count_transactions_called_with_internal_type_call
  , COUNT(*) FILTER (WHERE is_transaction_with_internal_call_type_call_or_delegate) AS count_transactions_called_with_internal_type_call_or_delegate

  , SUM(sum_trace_gas_used_minus_subtraces) FILTER (
        WHERE tx_success AND is_transaction_with_internal_call_type_call
        ) AS sum_trace_gas_used_minus_subtraces_tx_success_called_with_internal_type_call
, SUM(sum_tx_l2_gas_used_amortized_by_call) FILTER (
        WHERE tx_success AND is_transaction_with_internal_call_type_call
        ) AS sum_tx_l2_gas_used_amortized_by_call_tx_success_called_with_internal_type_call
, SUM(sum_tx_l2_fee_native_minus_subtraces) FILTER (
        WHERE tx_success AND is_transaction_with_internal_call_type_call
        ) AS sum_tx_l2_fee_native_minus_subtraces_tx_success_called_with_internal_type_call
, SUM(sum_tx_l2_fee_native_amortized_by_call) FILTER (
        WHERE tx_success AND is_transaction_with_internal_call_type_call
        ) AS sum_tx_l2_fee_native_amortized_by_call_tx_success_called_with_internal_type_call
, SUM(sum_tx_fee_native_amortized_by_call) FILTER (
        WHERE tx_success AND is_transaction_with_internal_call_type_call
        ) AS sum_tx_fee_native_amortized_by_call_tx_success_called_with_internal_type_call

--count by trace type
,SUM(count_traces_type_all_types) AS count_traces_type_all_types
,SUM(count_traces_trace_type_call) AS count_traces_trace_type_call
,SUM(count_traces_trace_type_suicide) AS count_traces_trace_type_suicide
,SUM(count_traces_trace_type_create) AS count_traces_trace_type_create
,SUM(count_traces_trace_type_create2) AS count_traces_trace_type_create2
,SUM(count_traces_trace_type_create_any) AS count_traces_trace_type_create_any

FROM aggregated_traces_tr_to_hash
WHERE 
  NOT is_system_transaction

GROUP BY
  dt
  , chain
  , network
  , chain_id
  , trace_to_address
