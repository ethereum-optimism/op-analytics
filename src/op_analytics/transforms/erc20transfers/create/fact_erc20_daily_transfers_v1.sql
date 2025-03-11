CREATE TABLE IF NOT EXISTS _placeholder_
(
    `chain` String,
    `chain_id` Int32,
    `dt` Date,
    `network` String,
    `contract_address` FixedString(42),
    `num_transfers` UInt64,
    `num_transactions` UInt64,
    `num_blocks` UInt64,
    `num_from_addresses` UInt64,
    `num_to_addresses` UInt64,
    `amount_raw` UInt256,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain, chain_id, dt, network, contract_address)
