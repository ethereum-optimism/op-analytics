CREATE TABLE IF NOT EXISTS OUTPUT_TABLE
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `trace_to_address` FixedString(42),
    `trace_from_address` FixedString(42),
    `tx_method_id` String,
    `count_distinct_transactions` AggregateFunction(uniq, String),
    `count_distinct_success_transactions` AggregateFunction(uniq, Nullable(String)),
    `sum_tx_l2_gas_used` AggregateFunction(sum, UInt64),
    `sum_tx_l2_gas_used_tx_success` AggregateFunction(sum, UInt64),
    `sum_tx_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_fee_native_tx_success` AggregateFunction(sum, Float64),
    `sum_tx_l1_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_l1_fee_native_tx_success` AggregateFunction(sum, Float64),
    `sum_tx_l2_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_l2_fee_native_tx_success` AggregateFunction(sum, Float64),
    `sum_tx_l2_priority_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_l2_priority_fee_native_tx_success` AggregateFunction(sum, Float64),
    `sum_tx_l2_base_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_l2_base_fee_native_tx_success` AggregateFunction(sum, Float64),
    `sum_tx_l2_legacy_extra_fee_native` AggregateFunction(sum, Float64),
    `sum_tx_l2_legacy_extra_fee_native_tx_success` AggregateFunction(sum, Float64),

    `weighted_sum_tx_l2_gas_price_gwei_called` AggregateFunction(sum, Float64),
    `weighted_sum_tx_l2_priority_gas_price_gwei_called` AggregateFunction(sum, Float64),
    `weighted_sum_tx_l2_legacy_extra_gas_price_gwei_called` AggregateFunction(sum, Float64),
    `weighted_sum_tx_l1_base_gas_price_gwei_called` AggregateFunction(sum, Float64),
    `weighted_sum_tx_l1_blob_base_gas_price_gwei_called` AggregateFunction(sum, Float64),

    `sum_tx_input_byte_length_called` AggregateFunction(sum, UInt32),
    `sum_tx_input_zero_bytes_called` AggregateFunction(sum, UInt32),
    `sum_tx_input_nonzero_bytes_called` AggregateFunction(sum, Int64),

    `sum_tx_l1_base_scaled_size_called` AggregateFunction(sum, Float64),
    `sum_tx_l1_blob_scaled_size_called` AggregateFunction(sum, Float64),
    `sum_tx_l1_gas_used_unified_called` AggregateFunction(sum, UInt64),
    `sum_tx_estimated_size_called` AggregateFunction(sum, UInt64),

    `sum_trace_gas_used` AggregateFunction(sum, UInt64),
    `sum_trace_gas_used_tx_success` AggregateFunction(sum, UInt64),

    `sum_trace_gas_used_minus_subtraces` AggregateFunction(sum, Float64),
    `sum_trace_gas_used_minus_subtraces_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l2_gas_used_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l2_gas_used_amortized_by_call_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l1_gas_used_unified_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l1_base_scaled_size_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l1_blob_scaled_size_amortized_by_call` AggregateFunction(sum, Float64),

    `sum_tx_estimated_size_amortized_by_call` AggregateFunction(sum, Float64),

    `sum_tx_l2_fee_native_minus_subtraces` AggregateFunction(sum, Float64),
    `sum_tx_l2_fee_native_minus_subtraces_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l2_base_fee_native_minus_subtraces` AggregateFunction(sum, Float64),
    `sum_tx_l2_priority_fee_native_minus_subtraces` AggregateFunction(sum, Float64),
    `sum_tx_l2_legacy_base_fee_native_minus_subtraces` AggregateFunction(sum, Float64),

    `sum_tx_fee_native_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_fee_native_amortized_by_call_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l2_fee_native_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l2_fee_native_amortized_by_call_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l1_fee_native_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l1_fee_native_amortized_by_call_tx_success` AggregateFunction(sum, Float64),

    `sum_tx_l2_base_fee_native_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_l2_priority_fee_native_amortized_by_call` AggregateFunction(sum, Float64),
    `sum_tx_fee_native_l1_amortized_l2_minus_subtraces` AggregateFunction(sum, Float64),

    `count_top_level_calls` AggregateFunction(sum, UInt8),
    `count_internal_calls_all_types` AggregateFunction(sum, UInt8),
    `count_internal_calls_all_types_trace_success` AggregateFunction(sum, UInt8),
    `count_internal_calls_static_type` AggregateFunction(sum, UInt8),
    `count_internal_calls_static_type_trace_success` AggregateFunction(sum, UInt8),
    `count_internal_calls_delegate_type` AggregateFunction(sum, UInt8),
    `count_internal_calls_delegate_type_trace_success` AggregateFunction(sum, UInt8),
    `count_internal_calls_call_type` AggregateFunction(sum, UInt8),
    `count_internal_calls_call_type_trace_success` AggregateFunction(sum, UInt8),

    `count_transactions_called_with_internal_type_call` AggregateFunction(uniq, Nullable(String)),
    `count_transactions_called_with_internal_type_call_or_delegate` AggregateFunction(uniq, Nullable(String)),

    `count_traces_type_all_types` AggregateFunction(sum, UInt8),
    `count_traces_trace_type_call` AggregateFunction(sum, UInt8),
    `count_traces_trace_type_suicide` AggregateFunction(sum, UInt8),
    `count_traces_trace_type_create` AggregateFunction(sum, UInt8),
    `count_traces_trace_type_create2` AggregateFunction(sum, UInt8),
    `count_traces_trace_type_create_any` AggregateFunction(sum, UInt8),
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, trace_to_address, trace_from_address, tx_method_id)
TTL dt + INTERVAL 6 MONTH DELETE;
