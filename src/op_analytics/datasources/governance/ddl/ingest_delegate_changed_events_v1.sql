CREATE TABLE IF NOT EXISTS _placeholder_
(
    `chain_id` Int64,
    `address` String,
    `block_number` Int64,
    `block_hash` Nullable(String),
    `log_index` Int64,
    `transaction_index` Int64,
    `transaction_hash` Nullable(String),
    `delegator` Nullable(String),
    `from_delegate` Nullable(String),
    `to_delegate` Nullable(String),
    INDEX block_number_idx block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (chain_id, address, block_number, log_index, transaction_index)
