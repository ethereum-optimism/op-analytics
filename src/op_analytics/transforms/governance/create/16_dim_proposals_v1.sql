CREATE TABLE IF NOT EXISTS _placeholder_
(
    `proposal_id` String,
    `ordinal`                 UInt64,
    `created_block`          UInt64,
    `created_block_ts`        DateTime,
    `proposal_creator`        String,
    `proposal_description`    String,
    `proposal_type`           String,
    `proposal_results`        String,
    `proposal_name`           String,
    `quorum_perc`             Float64,
    `proposal_type_id`        UInt32,
    `approval_threshold_perc` Float64,
    `start_block_ts`          DateTime,
    `end_block_ts`            DateTime,
    `total_against_votes`     UInt256,
    `total_for_votes`         UInt256,
    `total_abstain_votes`     UInt256,
    `total_votes`             UInt256,
    `pct_against`             Float64,
    `pct_for`                 Float64,
    `pct_abstain`             Float64,
    `executed_block_ts`      DateTime,
    INDEX proposal_id_idx proposal_id TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (proposal_id);