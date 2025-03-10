CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `transaction_hash` FixedString(66),
    `log_index`        UInt16,
    `delegate`         String,
    `previous_balance` UInt256,
    `new_balance`      UInt256,
    `delegation_amount`UInt256,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, transaction_hash, log_index)
