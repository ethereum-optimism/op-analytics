CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `transaction_hash` FixedString(66),
    `delegator`        String,
    `undelegated_from` String,
    `delegated_to`     String,
    `is_first_time_delegation` UInt8,
    `is_delegate_change` UInt8,
    `is_self_delegation` UInt8,
    `votes_changed_log_index` UInt16,
    `delegated_to_previous_balance` Nullable(UInt256),
    `delegated_to_new_balance`      Nullable(UInt256),
    `delegated_to_amount`           Nullable(Int256),
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, transaction_hash)