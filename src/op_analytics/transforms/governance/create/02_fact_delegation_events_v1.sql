CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `transaction_hash` FixedString(66),
    `log_index`        UInt16,
    `delegator`        String,
    `delegate`         String,
    `undelegated_from` String,
    `is_first_time_delegation` UInt8,
    `is_delegate_change` UInt8,
    `is_self_delegation` UInt8,
    `previous_balance` Float64,
    `new_balance`      Float64,
    `delegation_amount` Float64,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, transaction_hash, log_index)