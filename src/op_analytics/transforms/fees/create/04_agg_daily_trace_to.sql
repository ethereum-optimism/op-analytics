CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `trace_to_address` FixedString(42),
    
    -- Aggregate transaction metrics
    `count_transactions_called` UInt32,
    `count_transactions_called_tx_success` UInt32,
    
    `count_distinct_blocks_called` UInt32,
    `count_distinct_tx_from_addresses` UInt32,
    `count_distinct_tx_to_addresses` UInt32,
    `count_distinct_tx_method_ids` UInt32,
    
    -- Transaction-Level gas & fees
    `sum_tx_l2_gas_used` UInt256,
    `sum_tx_l2_gas_used_tx_success` UInt256,
    
    `sum_tx_fee_native` Float64,
    `sum_tx_fee_native_tx_success` Float64,
    
    `sum_tx_l1_fee_native` Float64,
    `sum_tx_l1_fee_native_tx_success` Float64,
    
    `sum_tx_l2_fee_native` Float64,
    `sum_tx_l2_fee_native_tx_success` Float64,
    
    `sum_tx_l2_priority_fee_native` Float64,
    `sum_tx_l2_priority_fee_native_tx_success` Float64,
    
    `sum_tx_l2_base_fee_native` Float64,
    `sum_tx_l2_base_fee_native_tx_success` Float64,
    
    `sum_tx_l2_legacy_extra_fee_native` Float64,
    `sum_tx_l2_legacy_extra_fee_native_tx_success` Float64,
    
    -- Transaction-Level Gas Prices
    `avg_tx_l2_gas_price_gwei_called` Float64,
    `avg_tx_l2_priority_gas_price_gwei_called` Float64,
    `avg_tx_l2_legacy_extra_gas_price_gwei_called` Float64,
    `avg_tx_l1_base_gas_price_gwei_called` Float64,
    `avg_tx_l1_blob_base_gas_price_gwei_called` Float64,
    
    -- Transaction-Level transaction sizes
    `sum_tx_input_byte_length_called` UInt64,
    `sum_tx_input_zero_bytes_called` UInt64,
    `sum_tx_input_nonzero_bytes_called` UInt64,
    `sum_tx_l1_base_scaled_size_called` Float64,
    `sum_tx_l1_blob_scaled_size_called` Float64,
    `sum_tx_l1_gas_used_unified_called` UInt64,
    `sum_tx_estimated_size_called` UInt64,
    
    -- Sum Internal Trace Gas
    `sum_trace_gas_used` Float64,
    `sum_trace_gas_used_tx_success` Float64,
    
    `sum_trace_gas_used_minus_subtraces` Float64,
    `sum_trace_gas_used_minus_subtraces_tx_success` Float64,
    
    `sum_tx_l2_gas_used_amortized_by_call` Float64,
    `sum_tx_l2_gas_used_amortized_by_call_tx_success` Float64,
    
    `sum_tx_l1_gas_used_unified_amortized_by_call` Float64,
    `sum_tx_l1_base_scaled_size_amortized_by_call` Float64,
    `sum_tx_l1_blob_scaled_size_amortized_by_call` Float64,
    `sum_tx_estimated_size_amortized_by_call` Float64,
    
    `sum_tx_l2_fee_native_minus_subtraces` Float64,
    `sum_tx_l2_fee_native_minus_subtraces_tx_success` Float64,
    
    `sum_tx_l2_base_fee_native_minus_subtraces` Float64,
    `sum_tx_l2_priority_fee_native_minus_subtraces` Float64,
    `sum_tx_l2_legacy_base_fee_native_minus_subtraces` Float64,
    
    `sum_tx_fee_native_amortized_by_call` Float64,
    `sum_tx_fee_native_amortized_by_call_tx_success` Float64,
    
    `sum_tx_l2_fee_native_amortized_by_call` Float64,
    `sum_tx_l2_fee_native_amortized_by_call_tx_success` Float64,
    
    `sum_tx_l1_fee_native_amortized_by_call` Float64,
    `sum_tx_l1_fee_native_amortized_by_call_tx_success` Float64,
    
    `sum_tx_l2_base_fee_native_amortized_by_call` Float64,
    `sum_tx_l2_priority_fee_native_amortized_by_call` Float64,
    
    `sum_tx_fee_native_l1_amortized_l2_minus_subtraces` Float64,
    
    -- Calls
    `count_top_level_calls` UInt32,
    `count_internal_calls_all_types` UInt32,
    `count_internal_calls_all_types_trace_success` UInt32,
    `count_internal_calls_static_type` UInt32,
    `count_internal_calls_static_type_trace_success` UInt32,
    `count_internal_calls_delegate_type` UInt32,
    `count_internal_calls_delegate_type_trace_success` UInt32,
    `count_internal_calls_call_type` UInt32,
    `count_internal_calls_call_type_trace_success` UInt32,
    
    -- Experimental
    `count_transactions_called_with_internal_type_call` UInt32,
    `count_transactions_called_with_internal_type_call_or_delegate` UInt32,
    
    `sum_trace_gas_used_minus_subtraces_tx_success_called_with_internal_type_call` Float64,
    `sum_tx_l2_gas_used_amortized_by_call_tx_success_called_with_internal_type_call` Float64,
    `sum_tx_l2_fee_native_minus_subtraces_tx_success_called_with_internal_type_call` Float64,
    `sum_tx_l2_fee_native_amortized_by_call_tx_success_called_with_internal_type_call` Float64,
    `sum_tx_fee_native_amortized_by_call_tx_success_called_with_internal_type_call` Float64,
    
    -- Count by trace type
    `count_traces_type_all_types` UInt32,
    `count_traces_trace_type_call` UInt32,
    `count_traces_trace_type_suicide` UInt32,
    `count_traces_trace_type_create` UInt32,
    `count_traces_trace_type_create2` UInt32,
    `count_traces_trace_type_create_any` UInt32,
    
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, trace_to_address)
