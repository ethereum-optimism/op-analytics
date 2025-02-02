CREATE TABLE IF NOT EXISTS transforms_{db}.{table}
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1))
    `block_number` UInt64 CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `transaction_index` Int64,
    `log_index` Int64,
    `contract_address` FixedString(42),
    INDEX dt_idx chain TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, ch

ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, block_number, transaction_hash, transaction_index, log_index))
