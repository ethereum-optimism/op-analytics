CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,

    `distinct_from_addresses` UInt64,
    `total_l2_gas_used` UInt64,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network)
