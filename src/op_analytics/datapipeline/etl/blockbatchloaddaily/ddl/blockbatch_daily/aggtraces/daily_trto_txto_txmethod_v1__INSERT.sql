/**
Aggregation grain is tr_to_address, tx_to_address, tx_method_id
*/

WITH

refined_traces AS (
  SELECT
    dt
    , chain
    , chain_id
    , network
    , accurateCast(trace_to_address, 'FixedString(42)') AS trace_to_address
    , accurateCast(trace_from_address, 'FixedString(42)') AS trace_from_address
    , accurateCast(transaction_hash, 'String') AS transaction_hash
    , accurateCast(block_number, 'UInt64') AS block_number
    , accurateCast(num_traces_in_txn, 'UInt32') AS num_traces_in_txn
    , accurateCast(trace_gas_used, 'UInt64') AS trace_gas_used
    , accurateCast(sum_subtraces_gas_used, 'Float64') AS sum_subtraces_gas_used
    , accurateCast(gas_used_minus_subtraces, 'Float64') AS gas_used_minus_subtraces
    , accurateCast(tx_l2_gas_used_amortized_by_call, 'Float64') AS tx_l2_gas_used_amortized_by_call
    , accurateCast(coalesce(tx_l1_gas_used_unified_amortized_by_call, 0), 'Float64') AS tx_l1_gas_used_unified_amortized_by_call
    , accurateCast(coalesce(tx_l1_base_scaled_size_amortized_by_call, 0), 'Float64') AS tx_l1_base_scaled_size_amortized_by_call
    , accurateCast(coalesce(tx_l1_blob_scaled_size_amortized_by_call, 0), 'Float64') AS tx_l1_blob_scaled_size_amortized_by_call
    , accurateCast(coalesce(tx_estimated_size_amortized_by_call, 0), 'Float64') AS tx_estimated_size_amortized_by_call
    , accurateCast(tx_l2_fee_native_minus_subtraces, 'Float64') AS tx_l2_fee_native_minus_subtraces
    , accurateCast(tx_l2_base_fee_native_minus_subtraces, 'Float64') AS tx_l2_base_fee_native_minus_subtraces
    , accurateCast(tx_l2_priority_fee_native_minus_subtraces, 'Float64') AS tx_l2_priority_fee_native_minus_subtraces
    , accurateCast(tx_l2_legacy_base_fee_native_minus_subtraces, 'Float64') AS tx_l2_legacy_base_fee_native_minus_subtraces
    , accurateCast(tx_fee_native_amortized_by_call, 'Float64') AS tx_fee_native_amortized_by_call
    , accurateCast(tx_l2_fee_native_amortized_by_call, 'Float64') AS tx_l2_fee_native_amortized_by_call
    , accurateCast(tx_l1_fee_native_amortized_by_call, 'Float64') AS tx_l1_fee_native_amortized_by_call
    , accurateCast(tx_l2_base_fee_native_amortized_by_call, 'Float64') AS tx_l2_base_fee_native_amortized_by_call
    , accurateCast(tx_l2_priority_fee_native_amortized_by_call, 'Float64') AS tx_l2_priority_fee_native_amortized_by_call

    , if(trace_address = '' AND call_type != '', 1, 0) AS is_top_level_call

    -- non-null trace addresses, null is the transaction-level call.
    , if(trace_address != '' AND call_type != '', 1, 0) AS is_internal_call_all_types
    , if(trace_address != '' AND call_type != '' AND trace_success, 1, 0) AS is_internal_call_all_types_trace_success

    --static calls only read state, and can not write
    , if(trace_address != '' AND call_type = 'staticcall', 1, 0) AS is_internal_call_static_type
    , if(trace_address != '' AND call_type = 'staticcall' AND trace_success, 1, 0) AS is_internal_call_static_type_trace_success

    --delegate calls call a function on another contract
    , if(trace_address != '' AND call_type = 'delegatecall', 1, 0) AS is_internal_call_delegate_type
    , if(trace_address != '' AND call_type = 'delegatecall' AND trace_success, 1, 0) AS is_internal_call_delegate_type_trace_success

    --normal function calls
    , if(trace_address != '' AND call_type = 'call', 1, 0) AS is_internal_call_call_type
    , if(trace_address != '' AND call_type = 'call' AND trace_success, 1, 0) AS is_internal_call_call_type_trace_success

    , if(trace_type = 'call', 1, 0) AS is_type_call
    , if(trace_type = 'suicide', 1, 0) AS is_type_suicide
    , if(trace_type = 'create', 1, 0) AS is_type_create
    , if(trace_type = 'create2', 1, 0) AS is_type_create2
    , if(trace_type LIKE 'create%', 1, 0) AS is_type_create_any

    -- Transaction Fields

    , accurateCast(tx_success, 'Bool') AS tx_success
    , accurateCast(tx_from_address, 'FixedString(42)') AS tx_from_address
    , accurateCast(tx_to_address, 'FixedString(42)') AS tx_to_address
    , accurateCast(tx_method_id, 'String') AS tx_method_id
    , accurateCast(tx_l2_gas_used, 'UInt64') AS tx_l2_gas_used
    , accurateCast(tx_fee_native, 'Float64') AS tx_fee_native

    , accurateCast(tx_l1_fee_native, 'Float64') AS tx_l1_fee_native
    , accurateCast(tx_l2_fee_native, 'Float64') AS tx_l2_fee_native
    , accurateCast(tx_l2_priority_fee_native, 'Float64') AS tx_l2_priority_fee_native
    , accurateCast(tx_l2_base_fee_native, 'Float64') AS tx_l2_base_fee_native
    , accurateCast(tx_l2_legacy_extra_fee_native, 'Float64') AS tx_l2_legacy_extra_fee_native

    , accurateCast(tx_l2_gas_price_gwei, 'Float64') AS tx_l2_gas_price_gwei
    , accurateCast(tx_l2_priority_gas_price_gwei, 'Float64') AS tx_l2_priority_gas_price_gwei
    , accurateCast(tx_l2_legacy_extra_gas_price_gwei, 'Float64') AS tx_l2_legacy_extra_gas_price_gwei
    , accurateCast(coalesce(tx_l1_base_gas_price_gwei, 0), 'Float64') AS tx_l1_base_gas_price_gwei
    , accurateCast(coalesce(tx_l1_blob_base_gas_price_gwei, 0), 'Float64') AS tx_l1_blob_base_gas_price_gwei

    , accurateCast(coalesce(tx_l1_gas_used_unified, 0), 'Float64') AS tx_l1_gas_used_unified
    , accurateCast(coalesce(tx_l1_base_scaled_size, 0), 'Float64') AS tx_l1_base_scaled_size
    , accurateCast(coalesce(tx_l1_blob_scaled_size, 0), 'Float64') AS tx_l1_blob_scaled_size
    , accurateCast(coalesce(tx_estimated_size, 0), 'Float64') AS tx_estimated_size

    , accurateCast(tx_input_byte_length, 'UInt32') AS tx_input_byte_length
    , accurateCast(tx_input_zero_bytes, 'UInt32') AS tx_input_zero_bytes
    , accurateCast(tx_input_nonzero_bytes, 'UInt32') AS tx_input_nonzero_bytes

  FROM INPUT_BLOCKBATCH('blockbatch/refined_traces/refined_traces_fees_v2')
)


SELECT
  tr.dt
  , tr.chain
  , tr.chain_id
  , tr.network
  , tr.trace_to_address
  , tr.tx_to_address AS tx_to_address
  , tr.tx_method_id AS tx_method_id

  , uniq(tr.trace_from_address) AS count_distinct_trace_from_addresses
  , uniq(tr.tx_from_address) AS count_distinct_tx_from_addresses

  -- Aggregate transaction metrics, in transactions when a given contract was called
  /*
    Note: We can NOT sum these up at the project level, since a project may have
    many of its contracts called in a single transaction.

    For these use cases (i.e. project impact metrics), we can aggregate `aggregated_traces_tr_to_hash`
    at the project name level, only after all trace_to_address entries are mapped to their respective
    project name (i.e. a transaction may call 10 contracts, but this may only represent 3 projects).
  */

  -- Transaction Counts
  , uniq(tr.transaction_hash) AS count_distinct_transactions
  , uniq(CASE WHEN tr.tx_success THEN tr.transaction_hash END) AS count_distinct_success_transactions

  -- Transaction Level Gas and Fees
  , sum(tr.tx_l2_gas_used) AS sum_tx_l2_gas_used
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_gas_used ELSE 0 END) AS sum_tx_l2_gas_used_tx_success

  , sum(tr.tx_fee_native) AS sum_tx_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_fee_native ELSE 0 END) AS sum_tx_fee_native_tx_success

  , sum(tr.tx_l1_fee_native) AS sum_tx_l1_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l1_fee_native ELSE 0 END) AS sum_tx_l1_fee_native_tx_success

  , sum(tr.tx_l2_fee_native) AS sum_tx_l2_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_fee_native ELSE 0 END) AS sum_tx_l2_fee_native_tx_success

  , sum(tr.tx_l2_priority_fee_native) AS sum_tx_l2_priority_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_priority_fee_native ELSE 0 END) AS sum_tx_l2_priority_fee_native_tx_success

  , sum(tr.tx_l2_base_fee_native) AS sum_tx_l2_base_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_base_fee_native ELSE 0 END) AS sum_tx_l2_base_fee_native_tx_success

  , sum(tr.tx_l2_legacy_extra_fee_native) AS sum_tx_l2_legacy_extra_fee_native
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_legacy_extra_fee_native ELSE 0 END) AS sum_tx_l2_legacy_extra_fee_native_tx_success

  -- Transaction-Level Gas Prices
  , sum(tr.tx_l2_gas_price_gwei * tr.tx_l2_gas_used) AS weighted_sum_tx_l2_gas_price_gwei_called
  , sum(tr.tx_l2_priority_gas_price_gwei * tr.tx_l2_gas_used) AS weighted_sum_tx_l2_priority_gas_price_gwei_called
  , sum(tr.tx_l2_legacy_extra_gas_price_gwei * tr.tx_l2_gas_used) AS weighted_sum_tx_l2_legacy_extra_gas_price_gwei_called
  , sum(tr.tx_l1_base_gas_price_gwei * tr.tx_l2_gas_used) AS weighted_sum_tx_l1_base_gas_price_gwei_called
  , sum(tr.tx_l1_blob_base_gas_price_gwei * tr.tx_l2_gas_used) AS weighted_sum_tx_l1_blob_base_gas_price_gwei_called

  -- Transaction-Level transaction sizes
  , sum(tr.tx_input_byte_length) AS sum_tx_input_byte_length_called
  , sum(tr.tx_input_zero_bytes) AS sum_tx_input_zero_bytes_called
  , sum(tr.tx_input_nonzero_bytes) AS sum_tx_input_nonzero_bytes_called
  , sum(tr.tx_l1_base_scaled_size) AS sum_tx_l1_base_scaled_size_called
  , sum(tr.tx_l1_blob_scaled_size) AS sum_tx_l1_blob_scaled_size_called
  , sum(tr.tx_l1_gas_used_unified) AS sum_tx_l1_gas_used_unified_called
  , sum(tr.tx_estimated_size) AS sum_tx_estimated_size_called

  -- Sum Internal Trace Gas
  /*
    Note: As opposed to transaction-level equivalent metrics, these CAN be summed up at a project level.
    These methodologies were designed to distribute gas usage and fees across each call, so that they
    still represent a "part of a whole," whereas transaction-level metrics do not.
  */

  , sum(tr.trace_gas_used) AS sum_trace_gas_used
  , sum(CASE WHEN tr.tx_success THEN tr.trace_gas_used ELSE 0 END) AS sum_trace_gas_used_tx_success

  , sum(tr.gas_used_minus_subtraces) AS sum_trace_gas_used_minus_subtraces
  , sum(CASE WHEN tr.tx_success THEN tr.gas_used_minus_subtraces ELSE 0 END) AS sum_trace_gas_used_minus_subtraces_tx_success

  , sum(tr.tx_l2_gas_used_amortized_by_call) AS sum_tx_l2_gas_used_amortized_by_call
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_gas_used_amortized_by_call ELSE 0 END) AS sum_tx_l2_gas_used_amortized_by_call_tx_success

  , sum(tr.tx_l1_gas_used_unified_amortized_by_call) AS sum_tx_l1_gas_used_unified_amortized_by_call
  , sum(tr.tx_l1_base_scaled_size_amortized_by_call) AS sum_tx_l1_base_scaled_size_amortized_by_call
  , sum(tr.tx_l1_blob_scaled_size_amortized_by_call) AS sum_tx_l1_blob_scaled_size_amortized_by_call
  , sum(tr.tx_estimated_size_amortized_by_call) AS sum_tx_estimated_size_amortized_by_call

  , sum(tr.tx_l2_fee_native_minus_subtraces) AS sum_tx_l2_fee_native_minus_subtraces
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_fee_native_minus_subtraces ELSE 0 END) AS sum_tx_l2_fee_native_minus_subtraces_tx_success

  , sum(tr.tx_l2_base_fee_native_minus_subtraces) AS sum_tx_l2_base_fee_native_minus_subtraces
  , sum(tr.tx_l2_priority_fee_native_minus_subtraces) AS sum_tx_l2_priority_fee_native_minus_subtraces
  , sum(tr.tx_l2_legacy_base_fee_native_minus_subtraces) AS sum_tx_l2_legacy_base_fee_native_minus_subtraces

  , sum(tr.tx_fee_native_amortized_by_call) AS sum_tx_fee_native_amortized_by_call
  , sum(CASE WHEN tr.tx_success THEN tr.tx_fee_native_amortized_by_call ELSE 0 END) AS sum_tx_fee_native_amortized_by_call_tx_success

  , sum(tr.tx_l2_fee_native_amortized_by_call) AS sum_tx_l2_fee_native_amortized_by_call
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l2_fee_native_amortized_by_call ELSE 0 END) AS sum_tx_l2_fee_native_amortized_by_call_tx_success

  , sum(tr.tx_l1_fee_native_amortized_by_call) AS sum_tx_l1_fee_native_amortized_by_call
  , sum(CASE WHEN tr.tx_success THEN tr.tx_l1_fee_native_amortized_by_call ELSE 0 END) AS sum_tx_l1_fee_native_amortized_by_call_tx_success

  , sum(tr.tx_l2_base_fee_native_amortized_by_call) AS sum_tx_l2_base_fee_native_amortized_by_call
  , sum(tr.tx_l2_priority_fee_native_amortized_by_call) AS sum_tx_l2_priority_fee_native_amortized_by_call

  , sum(tr.tx_l1_fee_native / tr.num_traces_in_txn + CAST(tr.trace_gas_used - coalesce(tr.sum_subtraces_gas_used, 0) AS Float64) * tr.tx_l2_gas_price_gwei * 1e-9) AS sum_tx_fee_native_l1_amortized_l2_minus_subtraces


  -- Calls
  , sum(tr.is_top_level_call) AS count_top_level_calls
  --count non-null trace addresses, null is the transaction-level call.
  , sum(tr.is_internal_call_all_types) AS count_internal_calls_all_types
  , sum(tr.is_internal_call_all_types_trace_success) AS count_internal_calls_all_types_trace_success
  --static calls only read state, and can not write
  , sum(tr.is_internal_call_static_type) AS count_internal_calls_static_type
  , sum(tr.is_internal_call_static_type_trace_success) AS count_internal_calls_static_type_trace_success
  --delegate calls call a function on another contract
  , sum(tr.is_internal_call_delegate_type) AS count_internal_calls_delegate_type
  , sum(tr.is_internal_call_delegate_type_trace_success) AS count_internal_calls_delegate_type_trace_success
  --normal function calls
  , sum(tr.is_internal_call_call_type) AS count_internal_calls_call_type
  , sum(tr.is_internal_call_call_type_trace_success) AS count_internal_calls_call_type_trace_success

  -- Experimental
  , uniq(CASE WHEN tr.is_internal_call_all_types THEN tr.transaction_hash END) AS count_transactions_called_with_internal_type_call
  , uniq(CASE WHEN tr.is_internal_call_all_types OR tr.is_internal_call_delegate_type THEN tr.transaction_hash END) AS count_transactions_called_with_internal_type_call_or_delegate


  --count by trace type
  , sum(1) AS count_traces_type_all_types
  , sum(tr.is_type_call) AS count_traces_trace_type_call
  , sum(tr.is_type_suicide) AS count_traces_trace_type_suicide
  , sum(tr.is_type_create) AS count_traces_trace_type_create
  , sum(tr.is_type_create2) AS count_traces_trace_type_create2
  , sum(tr.is_type_create_any) AS count_traces_trace_type_create_any

FROM refined_traces AS tr
GROUP BY
  tr.dt
  , tr.chain
  , tr.chain_id
  , tr.network
  , tr.trace_to_address
  , tr.tx_to_address
  , tr.tx_method_id
