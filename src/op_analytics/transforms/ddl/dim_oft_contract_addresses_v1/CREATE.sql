CREATE TABLE IF NOT EXISTS transforms.dim_oft_contract_addresses_v1
(
    `chain` String,
    `chain_id` Int32,
    `contract_address` FixedString(66),
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (chain, chain_id, contract_address)
