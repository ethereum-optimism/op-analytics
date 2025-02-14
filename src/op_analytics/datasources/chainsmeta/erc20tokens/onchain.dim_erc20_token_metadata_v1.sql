CREATE TABLE IF NOT EXISTS onchain.dim_erc20_token_metadata_v1
(
    `chain` String,
    `chain_id` Int32,
    `contract_address` FixedString(42),
    -- 
    -- Metadata
    `decimals` Int32,
    `symbol` String,
    `name` String,
    `total_supply` UInt256,
    --
    -- Block when metadata was last retrieved from RPC
    `block_number` UInt64,
    `block_timestamp` DateTime,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
    INDEX contract_address_idx contract_address TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree(block_timestamp)
ORDER BY (chain, chain_id, contract_address)
