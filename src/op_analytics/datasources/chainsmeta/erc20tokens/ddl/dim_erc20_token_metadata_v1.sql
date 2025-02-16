-- This dim table stores the token metadata only once for each token.
--
-- We don't insert data directly to this table. It is maintained using
-- a materialized view.

CREATE TABLE IF NOT EXISTS chainsmeta.dim_erc20_token_metadata_v1
(
    `chain` String,
    `chain_id` Int32,
    `contract_address` FixedString(42),
    -- 
    -- Metadata
    `decimals` UInt8,
    `symbol` String,
    `name` String,
    `total_supply` UInt256,
    --
    -- Block when metadata was retrieved from RPC
    `block_number` UInt64,
    `block_timestamp` DateTime,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
    INDEX contract_address_idx contract_address TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree(block_timestamp)
ORDER BY (chain, chain_id, contract_address)
