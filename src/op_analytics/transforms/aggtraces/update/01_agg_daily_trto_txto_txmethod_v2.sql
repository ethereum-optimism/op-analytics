SELECT
  dt
  , chain
  , chain_id
  , network
  , trace_to_address
  , tx_to_address
  , tx_method_id

  , uniqMerge(count_distinct_trace_from_addresses) AS count_distinct_trace_from_addresses

  -- Aggregate transaction metrics, in transactions when a given contract was called
  /*
    Note: We can NOT sum these up at the project level, since a project may have
    many of its contracts called in a single transaction.

    For these use cases (i.e. project impact metrics), we can aggregate `aggregated_traces_tr_to_hash`
    at the project name level, only after all trace_to_address entries are mapped to their respective
    project name (i.e. a transaction may call 10 contracts, but this may only represent 3 projects).
  */

  -- Transaction Counts
  , uniqMerge(count_distinct_transactions) AS count_distinct_transactions
  , uniqMerge(count_distinct_success_transactions) AS count_distinct_success_transactions

  -- Transaction Level Gas and Fees
  , sumMerge(sum_tx_l2_gas_used) AS sum_tx_l2_gas_used
  , sumMerge(sum_tx_l2_gas_used_tx_success) AS sum_tx_l2_gas_used_tx_success

  , sumMerge(sum_tx_fee_native) AS sum_tx_fee_native
  , sumMerge(sum_tx_fee_native_tx_success) AS sum_tx_fee_native_tx_success

  , sumMerge(sum_tx_l1_fee_native) AS sum_tx_l1_fee_native
  , sumMerge(sum_tx_l1_fee_native_tx_success) AS sum_tx_l1_fee_native_tx_success

  , sumMerge(sum_tx_l2_fee_native) AS sum_tx_l2_fee_native
  , sumMerge(sum_tx_l2_fee_native_tx_success) AS sum_tx_l2_fee_native_tx_success

  , sumMerge(sum_tx_l2_priority_fee_native) AS sum_tx_l2_priority_fee_native
  , sumMerge(sum_tx_l2_priority_fee_native_tx_success) AS sum_tx_l2_priority_fee_native_tx_success

  , sumMerge(sum_tx_l2_base_fee_native) AS sum_tx_l2_base_fee_native
  , sumMerge(sum_tx_l2_base_fee_native_tx_success) AS sum_tx_l2_base_fee_native_tx_success

  , sumMerge(sum_tx_l2_legacy_extra_fee_native) AS sum_tx_l2_legacy_extra_fee_native
  , sumMerge(sum_tx_l2_legacy_extra_fee_native_tx_success) AS sum_tx_l2_legacy_extra_fee_native_tx_success

  -- Transaction-Level Gas Prices
  , sumMerge(weighted_sum_tx_l2_gas_price_gwei_called) AS weighted_sum_tx_l2_gas_price_gwei_called
  , sumMerge(weighted_sum_tx_l2_priority_gas_price_gwei_called) AS weighted_sum_tx_l2_priority_gas_price_gwei_called
  , sumMerge(weighted_sum_tx_l2_legacy_extra_gas_price_gwei_called) AS weighted_sum_tx_l2_legacy_extra_gas_price_gwei_called
  , sumMerge(weighted_sum_tx_l1_base_gas_price_gwei_called) AS weighted_sum_tx_l1_base_gas_price_gwei_called
  , sumMerge(weighted_sum_tx_l1_blob_base_gas_price_gwei_called) AS weighted_sum_tx_l1_blob_base_gas_price_gwei_called

  -- Transaction-Level transaction sizes
  , sumMerge(sum_tx_input_byte_length_called) AS sum_tx_input_byte_length_called
  , sumMerge(sum_tx_input_zero_bytes_called) AS sum_tx_input_zero_bytes_called
  , sumMerge(sum_tx_input_nonzero_bytes_called) AS sum_tx_input_nonzero_bytes_called
  , sumMerge(sum_tx_l1_base_scaled_size_called) AS sum_tx_l1_base_scaled_size_called
  , sumMerge(sum_tx_l1_blob_scaled_size_called) AS sum_tx_l1_blob_scaled_size_called
  , sumMerge(sum_tx_l1_gas_used_unified_called) AS sum_tx_l1_gas_used_unified_called
  , sumMerge(sum_tx_estimated_size_called) AS sum_tx_estimated_size_called

  -- Sum Internal Trace Gas
  /*
    Note: As opposed to transaction-level equivalent metrics, these CAN be summed up at a project level.
    These methodologies were designed to distribute gas usage and fees across each call, so that they
    still represent a "part of a whole," whereas transaction-level metrics do not.
  */

  , sumMerge(sum_trace_gas_used) AS sum_trace_gas_used
  , sumMerge(sum_trace_gas_used_tx_success) AS sum_trace_gas_used_tx_success

  , sumMerge(sum_trace_gas_used_minus_subtraces) AS sum_trace_gas_used_minus_subtraces
  , sumMerge(sum_trace_gas_used_minus_subtraces_tx_success) AS sum_trace_gas_used_minus_subtraces_tx_success

  , sumMerge(sum_tx_l2_gas_used_amortized_by_call) AS sum_tx_l2_gas_used_amortized_by_call
  , sumMerge(sum_tx_l2_gas_used_amortized_by_call_tx_success) AS sum_tx_l2_gas_used_amortized_by_call_tx_success

  , sumMerge(sum_tx_l1_gas_used_unified_amortized_by_call) AS sum_tx_l1_gas_used_unified_amortized_by_call
  , sumMerge(sum_tx_l1_base_scaled_size_amortized_by_call) AS sum_tx_l1_base_scaled_size_amortized_by_call
  , sumMerge(sum_tx_l1_blob_scaled_size_amortized_by_call) AS sum_tx_l1_blob_scaled_size_amortized_by_call
  , sumMerge(sum_tx_estimated_size_amortized_by_call) AS sum_tx_estimated_size_amortized_by_call

  , sumMerge(sum_tx_l2_fee_native_minus_subtraces) AS sum_tx_l2_fee_native_minus_subtraces
  , sumMerge(sum_tx_l2_fee_native_minus_subtraces_tx_success) AS sum_tx_l2_fee_native_minus_subtraces_tx_success

  , sumMerge(sum_tx_l2_base_fee_native_minus_subtraces) AS sum_tx_l2_base_fee_native_minus_subtraces
  , sumMerge(sum_tx_l2_priority_fee_native_minus_subtraces) AS sum_tx_l2_priority_fee_native_minus_subtraces
  , sumMerge(sum_tx_l2_legacy_base_fee_native_minus_subtraces) AS sum_tx_l2_legacy_base_fee_native_minus_subtraces

  , sumMerge(sum_tx_fee_native_amortized_by_call) AS sum_tx_fee_native_amortized_by_call
  , sumMerge(sum_tx_fee_native_amortized_by_call_tx_success) AS sum_tx_fee_native_amortized_by_call_tx_success

  , sumMerge(sum_tx_l2_fee_native_amortized_by_call) AS sum_tx_l2_fee_native_amortized_by_call
  , sumMerge(sum_tx_l2_fee_native_amortized_by_call_tx_success) AS sum_tx_l2_fee_native_amortized_by_call_tx_success

  , sumMerge(sum_tx_l1_fee_native_amortized_by_call) AS sum_tx_l1_fee_native_amortized_by_call
  , sumMerge(sum_tx_l1_fee_native_amortized_by_call_tx_success) AS sum_tx_l1_fee_native_amortized_by_call_tx_success

  , sumMerge(sum_tx_l2_base_fee_native_amortized_by_call) AS sum_tx_l2_base_fee_native_amortized_by_call
  , sumMerge(sum_tx_l2_priority_fee_native_amortized_by_call) AS sum_tx_l2_priority_fee_native_amortized_by_call

  , sumMerge(sum_tx_fee_native_l1_amortized_l2_minus_subtraces) AS sum_tx_fee_native_l1_amortized_l2_minus_subtraces


  -- Calls
  , sumMerge(count_top_level_calls) AS count_top_level_calls
  --count non-null trace addresses, null is the transaction-level call.
  , sumMerge(count_internal_calls_all_types) AS count_internal_calls_all_types
  , sumMerge(count_internal_calls_all_types_trace_success) AS count_internal_calls_all_types_trace_success
  --static calls only read state, and can not write
  , sumMerge(count_internal_calls_static_type) AS count_internal_calls_static_type
  , sumMerge(count_internal_calls_static_type_trace_success) AS count_internal_calls_static_type_trace_success
  --delegate calls call a function on another contract
  , sumMerge(count_internal_calls_delegate_type) AS count_internal_calls_delegate_type
  , sumMerge(count_internal_calls_delegate_type_trace_success) AS count_internal_calls_delegate_type_trace_success
  --normal function calls
  , sumMerge(count_internal_calls_call_type) AS count_internal_calls_call_type
  , sumMerge(count_internal_calls_call_type_trace_success) AS count_internal_calls_call_type_trace_success

  -- Experimental
  , uniqMerge(count_transactions_called_with_internal_type_call) AS count_transactions_called_with_internal_type_call
  , uniqMerge(count_transactions_called_with_internal_type_call_or_delegate) AS count_transactions_called_with_internal_type_call_or_delegate


  --count by trace type
  , sumMerge(count_traces_type_all_types) AS count_traces_type_all_types
  , sumMerge(count_traces_trace_type_call) AS count_traces_trace_type_call
  , sumMerge(count_traces_trace_type_suicide) AS count_traces_trace_type_suicide
  , sumMerge(count_traces_trace_type_create) AS count_traces_trace_type_create
  , sumMerge(count_traces_trace_type_create2) AS count_traces_trace_type_create2
  , sumMerge(count_traces_trace_type_create_any) AS count_traces_trace_type_create_any

FROM blockbatch.aggregated_traces__traces_txgrain_v2 FINAL
WHERE dt = { dtparam: Date }
GROUP BY
  dt
  , chain
  , chain_id
  , network
  , trace_to_address
  , tx_to_address
  , tx_method_id
