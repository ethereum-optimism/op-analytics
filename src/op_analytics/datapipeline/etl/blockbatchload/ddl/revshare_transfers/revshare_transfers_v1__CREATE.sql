CREATE TABLE IF NOT EXISTS _placeholder_
(
    `chain` String,
    `dt` Date,
    `network` String,
    `chain_id` Int32,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `block_number` UInt64 CODEC(Delta(4), ZSTD(1)),
    `block_hash` FixedString(66),
    `transaction_hash` FixedString(66),
    `transaction_index` Int64,
    `trace_address` String,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `amount` UInt256,
    `transfer_type` String,
    `token_address` String,
    `revshare_from_chain` String,
    `revshare_from_chain_id` Nullable(Int32),
    `is_revshare_transfer` Bool,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX block_timestamp_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX revshare_from_chain_idx revshare_from_chain TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (chain, dt, network, chain_id, block_number, transaction_hash, transaction_index, trace_address) 