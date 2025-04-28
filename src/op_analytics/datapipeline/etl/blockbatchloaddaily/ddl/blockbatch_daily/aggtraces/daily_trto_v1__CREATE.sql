CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `trace_to_address` FixedString(42),
    `count_distinct_trace_from_addresses` UInt64,
    `count_distinct_tx_from_addresses` UInt64,
    `count_distinct_transactions` UInt64,
    `count_distinct_success_transactions` UInt64,
    `sum_tx_l2_gas_used` UInt64,
    `sum_tx_l2_gas_used_tx_success` UInt64,
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

    `weighted_sum_tx_l2_gas_price_gwei_called` Float64,
    `weighted_sum_tx_l2_priority_gas_price_gwei_called` Float64,
    `weighted_sum_tx_l2_legacy_extra_gas_price_gwei_called` Float64,
    `weighted_sum_tx_l1_base_gas_price_gwei_called` Float64,
    `weighted_sum_tx_l1_blob_base_gas_price_gwei_called` Float64,

    `sum_tx_input_byte_length_called` UInt64,
    `sum_tx_input_zero_bytes_called` UInt64,
    `sum_tx_input_nonzero_bytes_called` Int64,

    `sum_tx_l1_base_scaled_size_called` Float64,
    `sum_tx_l1_blob_scaled_size_called` Float64,
    `sum_tx_l1_gas_used_unified_called` UInt64,
    `sum_tx_estimated_size_called` UInt64,

    `sum_trace_gas_used` UInt64,
    `sum_trace_gas_used_tx_success` UInt64,

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

    `count_top_level_calls` UInt64,
    `count_internal_calls_all_types` UInt64,
    `count_internal_calls_all_types_trace_success` UInt64,
    `count_internal_calls_static_type` UInt64,
    `count_internal_calls_static_type_trace_success` UInt64,
    `count_internal_calls_delegate_type` UInt64,
    `count_internal_calls_delegate_type_trace_success` UInt64,
    `count_internal_calls_call_type` UInt64,
    `count_internal_calls_call_type_trace_success` UInt64,

    `count_transactions_called_with_internal_type_call` UInt64,
    `count_transactions_called_with_internal_type_call_or_delegate` UInt64,

    `count_traces_type_all_types` UInt64,
    `count_traces_trace_type_call` UInt64,
    `count_traces_trace_type_suicide` UInt64,
    `count_traces_trace_type_create` UInt64,
    `count_traces_trace_type_create2` UInt64,
    `count_traces_trace_type_create_any` UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, trace_to_address)
