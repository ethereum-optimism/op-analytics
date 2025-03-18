CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt`                      Date,
    `proposal_id`             String,
    `end_block_ts`            DateTime,
    `total_against_votes`     UInt256,
    `total_for_votes`         UInt256,
    `total_abstain_votes`     UInt256,
    `total_votes`             UInt256,
    `pct_against`             Float64,
    `pct_for`                 Float64,
    `pct_abstain`             Float64,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, proposal_id)