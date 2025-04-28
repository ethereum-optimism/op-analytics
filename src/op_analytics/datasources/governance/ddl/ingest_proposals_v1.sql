CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `proposal_id` String,
    `contract` Nullable(String),
    `proposer` Nullable(String),
    `description` Nullable(String),
    `ordinal` Nullable(Int64),
    `created_block` Nullable(Int64),
    `start_block` Nullable(Int64),
    `end_block` Nullable(Int64),
    `queued_block` Nullable(Int64),
    `cancelled_block` Nullable(Int64),
    `executed_block` Nullable(Int64),
    `proposal_data` Nullable(String),
    `proposal_data_raw` Nullable(String),
    `proposal_type` Nullable(String),
    `proposal_type_data` Nullable(String),
    `proposal_results` Nullable(String),
    `created_transaction_hash` Nullable(String),
    `cancelled_transaction_hash` Nullable(String),
    `queued_transaction_hash` Nullable(String),
    `executed_transaction_hash` Nullable(String),
    `proposal_type_id` Nullable(Int64),
    INDEX dt_idx dt TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, proposal_id)
