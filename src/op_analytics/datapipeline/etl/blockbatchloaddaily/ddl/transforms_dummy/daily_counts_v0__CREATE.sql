CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `num_addresses` UInt32,
    `num_transactions` UInt32,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt)
