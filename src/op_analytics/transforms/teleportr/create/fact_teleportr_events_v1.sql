CREATE TABLE IF NOT EXISTS _placeholder_
(
    `block_timestamp` DateTime,
    `dt` Date,
    `block_number` UInt64 CODEC(Delta(4), ZSTD(1)),
    `src_chain` String,
    `src_chain_id` Int32,
    `dst_chain` String,
    `dst_chain_id` Int32,
    `contract_address` FixedString(42),
    `transaction_hash` FixedString(66),
    `deposit_id` UInt32,
    `input_token_address` FixedString(42),
    `input_token_symbol` String,
    `input_amount_raw` UInt256,
    `input_amount` Float64,
    `output_token_address` FixedString(42),
    `output_token_symbol` String,
    `output_amount_raw` UInt256,
    `output_amount` Float64,
    `quote_timestamp` UInt256,
    `fill_deadline` UInt256,
    `exclusivity_deadline` UInt256,
    `recipient_address` FixedString(42),
    `relayer_address` FixedString(42),
    `depositor_address` FixedString(42),
    `integrator` String,
    `l2_fee_eth` Float64,
    `l1_fee_eth` Float64,
    `total_fee_eth` Float64,
    `log_index` UInt64 CODEC(Delta(4), ZSTD(1)),
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX src_chain_idx src_chain TYPE minmax GRANULARITY 1,
    INDEX dst_chain_idx dst_chain TYPE minmax GRANULARITY 1,
    INDEX deposit_id_idx deposit_id TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree()
ORDER BY (src_chain_id, dst_chain_id, dt, deposit_id, transaction_hash)
