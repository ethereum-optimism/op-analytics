CREATE TABLE IF NOT EXISTS _placeholder_
(
    `transaction_hash` String,
    `proposal_id` String,
    `voter` Nullable(String),
    `support` Nullable(Int64),
    `weight` Nullable(String),
    `reason` Nullable(String),
    `block_number` Int64,
    `params` Nullable(String),
    `start_block` Nullable(Int64),
    `description` Nullable(String),
    `proposal_data` Nullable(String),
    `proposal_type` Nullable(String),
    `contract` Nullable(String),
    `chain_id` Nullable(Int64),
    INDEX block_number_idx block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, proposal_id)
