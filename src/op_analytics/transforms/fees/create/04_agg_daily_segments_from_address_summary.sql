CREATE TABLE IF NOT EXISTS transforms_fees.agg_daily_segments_from_address_summary
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `wallet_segment` String,
    
    -- Base Metrics
    `wallet_count` UInt64,
    `avg_tx_count_per_wallet` Float64,
    `total_tx_count` UInt64,
    `total_success_tx_count` UInt64,
    `avg_success_tx_count_per_wallet` Float64,
    `overall_success_rate` Float64,
    
    -- Aggregated Distinct Counts (Averaged per Wallet)
    `avg_distinct_blocks_per_wallet` Float64,
    `avg_distinct_success_blocks_per_wallet` Float64,
    `avg_distinct_active_hours_per_wallet` Float64,
    `avg_distinct_to_address_per_wallet` Float64,
    `avg_distinct_success_to_address_per_wallet` Float64,
    `avg_distinct_method_id_per_wallet` Float64,
    
    -- Aggregated Ranges (Averaged per Wallet)
    `avg_active_block_range_per_wallet` Float64,
    `avg_active_nonce_range_per_wallet` Float64,
    `avg_active_time_range_sec_per_wallet` Float64,
    
    -- Aggregated Gas Usage (Totals)
    `total_l2_gas_used` UInt64,
    `total_success_l2_gas_used` UInt64,
    `total_l1_gas_used` UInt64,
    `total_success_l1_gas_used` UInt64,
    
    -- Aggregated Gas Usage (Avg Per Wallet)
    `avg_l2_gas_used_per_wallet` Float64,
    `avg_success_l2_gas_used_per_wallet` Float64,
    `avg_l1_gas_used_per_wallet` Float64,
    `avg_success_l1_gas_used_per_wallet` Float64,
    
    -- Aggregated Fees (Totals)
    `total_tx_fee_native` Float64,
    `total_success_tx_fee_native` Float64,
    `total_l2_fee_native` Float64,
    `total_success_l2_fee_native` Float64,
    `total_l2_base_fee_native` Float64,
    `total_success_l2_base_fee_native` Float64,
    `total_l2_priority_fee_native` Float64,
    `total_success_l2_priority_fee_native` Float64,
    `total_l2_legacy_extra_fee_native` Float64,
    `total_success_l2_legacy_extra_fee_native` Float64,
    `total_l1_fee_native` Float64,
    `total_success_l1_fee_native` Float64,
    `total_l1_base_fee_native` Float64,
    `total_success_l1_base_fee_native` Float64,
    `total_l1_blob_fee_native` Float64,
    `total_success_l1_blob_fee_native` Float64,
    
    -- Aggregated Fees (Avg Per Wallet)
    `avg_tx_fee_native_per_wallet` Float64,
    `avg_success_tx_fee_native_per_wallet` Float64,
    `avg_l2_fee_native_per_wallet` Float64,
    `avg_success_l2_fee_native_per_wallet` Float64,
    `avg_l2_base_fee_native_per_wallet` Float64,
    `avg_success_l2_base_fee_native_per_wallet` Float64,
    `avg_l2_priority_fee_native_per_wallet` Float64,
    `avg_success_l2_priority_fee_native_per_wallet` Float64,
    `avg_l2_legacy_extra_fee_native_per_wallet` Float64,
    `avg_success_l2_legacy_extra_fee_native_per_wallet` Float64,
    `avg_l1_fee_native_per_wallet` Float64,
    `avg_success_l1_fee_native_per_wallet` Float64,
    `avg_l1_base_fee_native_per_wallet` Float64,
    `avg_success_l1_base_fee_native_per_wallet` Float64,
    `avg_l1_blob_fee_native_per_wallet` Float64,
    `avg_success_l1_blob_fee_native_per_wallet` Float64,
    
    -- Aggregated Gas Prices (Averaged across Wallets)
    `avg_l2_gas_price_gwei_across_wallets` Float64,
    `avg_l2_priority_gas_price_gwei_across_wallets` Float64,
    `avg_l2_legacy_extra_gas_price_gwei_across_wallets` Float64,
    `avg_l1_base_gas_price_gwei_across_wallets` Float64,
    `avg_l1_blob_base_gas_price_gwei_across_wallets` Float64,

    -- Aggregated Input Data (Totals)
    `total_input_zero_bytes` UInt64,
    `total_success_input_zero_bytes` UInt64,
    `total_input_bytes_length` UInt64,
    `total_success_input_bytes_length` UInt64,
    `total_input_nonzero_bytes` UInt64,
    `total_success_input_nonzero_bytes` UInt64,
    
    -- Aggregated Input Data (Avg Per Wallet)
    `avg_input_zero_bytes_per_wallet` Float64,
    `avg_success_input_zero_bytes_per_wallet` Float64,
    `avg_input_bytes_length_per_wallet` Float64,
    `avg_success_input_bytes_length_per_wallet` Float64,
    `avg_input_nonzero_bytes_per_wallet` Float64,
    `avg_success_input_nonzero_bytes_per_wallet` Float64,
    
    -- Aggregated Estimated Size (Totals)
    `total_estimated_size` UInt64,
    `total_success_estimated_size` UInt64,
    
    -- Aggregated Estimated Size (Avg Per Wallet)
    `avg_estimated_size_per_wallet` Float64,
    `avg_success_estimated_size_per_wallet` Float64,
    
    -- Key Ratios per Transaction (Overall metrics)
    `overall_avg_tx_fee_native_per_tx` Float64,
    `overall_avg_l2_gas_per_tx` Float64,
    `overall_avg_l1_gas_per_tx` Float64,
    
    -- Per-Wallet Averaged Metrics
    `avg_wallet_success_rate` Float64,
    `avg_wallet_avg_fee_per_tx` Float64,
    `avg_wallet_avg_l2_gas_per_tx` Float64,
    `avg_wallet_avg_l1_gas_per_tx` Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (dt, chain, chain_id, network, wallet_segment)