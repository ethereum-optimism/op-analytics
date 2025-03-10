CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt`               Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `delegate`         String,
    `voting_power`     String,
    `transaction_index` UInt32,
    `log_index`        UInt16,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, log_index, delegate)