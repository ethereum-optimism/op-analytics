CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt`               Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `transaction_index` UInt32,
    `log_index`        UInt16,
    `delegate`         String,
    `voting_power`     String,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, transaction_index, log_index)