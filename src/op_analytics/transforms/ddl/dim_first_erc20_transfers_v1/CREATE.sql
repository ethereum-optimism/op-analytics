CREATE TABLE IF NOT EXISTS transforms.dim_first_erc20_transfers_v1
(
    `chain` String,
    `contract_address` FixedString(66),
    `first_transfer` DateTime,
    `row_version` Int64, 
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
    INDEX contract_address_idx contract_address TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree(row_version)
ORDER BY (chain, contract_address)
