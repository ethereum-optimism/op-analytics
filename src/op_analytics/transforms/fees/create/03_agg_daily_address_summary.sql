CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `from_address` FixedString(42),
    
    `count_transactions` UInt64,
    `count_success_transactions` UInt64,
    `count_distinct_blocks` UInt64,
    `count_distinct_success_blocks` UInt64,

    `min_block_number` UInt64,
    `max_block_number` UInt64,
    `active_block_range` Int64, 

    `min_nonce` UInt64,
    `max_nonce` UInt64,
    `active_nonce_range` Int64, 

    `min_block_timestamp` UInt64,
    `max_block_timestamp` UInt64,
    `active_time_range` Int64,
    `count_distinct_active_hours` UInt64,

    `count_distinct_to_address` UInt64,
    `count_distinct_success_to_address` UInt64,
    `count_distinct_method_id` UInt64,

    `sum_l2_gas_used` UInt256,
    `sum_success_l2_gas_used` UInt256,

    `sum_l1_gas_used_unified` UInt256,
    `sum_success_l1_gas_used_unified` UInt256,
    
    `sum_tx_fee_native` Float64,
    `sum_success_tx_fee_native` Float64,

    `sum_l2_fee_native` Float64,
    `sum_success_l2_fee_native` Float64,

    `sum_l2_base_fee_native` Float64,
    `sum_success_l2_base_fee_native` Float64,

    `sum_l2_priority_fee_native` Float64,
    `sum_success_l2_priority_fee_native` Float64,

    `sum_l2_legacy_extra_fee_native` Float64,
    `sum_success_l2_legacy_extra_fee_native` Float64,
    
    `sum_l1_fee_native` Float64,
    `sum_success_l1_fee_native` Float64,

    `sum_l1_base_fee_native` Float64,
    `sum_success_l1_base_fee_native` Float64,

    `sum_l1_blob_fee_native` Float64,
    `sum_sucess_l1_blob_fee_native` Float64

    `avg_l2_gas_price_gwei` Float64,
    `avg_l2_priority_gas_price_gwei` Float64,
    `avg_l2_legacy_extra_gas_price_gwei` Float64,
    `avg_l1_base_gas_price_gwei` Float64,
    `avg_l1_blob_base_gas_price_gwei` Float64,
    
    `sum_input_zero_bytes` UInt64,
    `sum_success_input_zero_bytes` UInt64,

    `sum_input_bytes_length` UInt64,
    `sum_success_input_bytes_length` UInt64,

    `sum_input_nonzero_bytes` UInt64,
    `sum_success_input_nonzero_bytes` UInt64
    
    `sum_estimated_size` UInt64,
    `sum_success_estimated_size` UInt64,
    
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, from_address)
