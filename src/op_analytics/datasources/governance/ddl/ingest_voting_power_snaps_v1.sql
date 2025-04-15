CREATE TABLE IF NOT EXISTS _placeholder_
(
    `id` String,
    `delegate` Nullable(String),
    `balance` Nullable(String),
    `block_number` Int64,
    `ordinal` Nullable(Int64),
    `transaction_index` Int64,
    `log_index` Int64,
    `contract` Nullable(String),
    INDEX block_number_idx block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (id, block_number, transaction_index, log_index)
