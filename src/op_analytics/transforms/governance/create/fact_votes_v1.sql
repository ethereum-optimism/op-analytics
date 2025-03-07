CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `block_timestamp`  DateTime,
    `block_number`     UInt64,
    `transaction_hash` FixedString(66),
    `voter_address`    String,
    `proposal_id`      UInt64,
    `decision`         String,
    `reason`           String,
    `voting_power`     Float64,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, block_timestamp, block_number, transaction_hash, voter_address, proposal_id)