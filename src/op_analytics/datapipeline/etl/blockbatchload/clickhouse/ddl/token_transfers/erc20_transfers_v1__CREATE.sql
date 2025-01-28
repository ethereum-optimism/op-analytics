CREATE TABLE IF NOT EXISTS blockbatch.token_transfers__erc20_transfers_v1
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
    `log_index` Int64,
    `contract_address` FixedString(42),
    `amount` UInt256,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX block_timestamp_idx block_timestamp TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (chain, dt, network, chain_id, block_number, transaction_hash, transaction_index, log_index)
