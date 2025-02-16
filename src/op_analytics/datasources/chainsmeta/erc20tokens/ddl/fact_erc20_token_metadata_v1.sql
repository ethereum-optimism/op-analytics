-- The fact table stores the token metadata at most one per day. 
-- The "process_dt" value in the ORDER BY key.
--
-- This table is used as a trigger for the dim table materialized
-- view.

CREATE TABLE IF NOT EXISTS chainsmeta.fact_erc20_token_metadata_v1
(
    process_dt Date,
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
ORDER BY (chain, chain_id, contract_address, process_dt)
