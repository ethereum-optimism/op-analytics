CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt`                      Date,
    `proposal_id`             String,
    `ordinal`                 UInt64,
    `created_block`           UInt64,
    `created_block_ts`        DateTime,
    `proposal_creator`        String,
    `proposal_description`    String,
    `proposal_type`           String,
    `proposal_results`        String,
    `proposal_name`           String,
    `quorum_perc`             Float64,
    `proposal_type_id`        UInt32,
    `approval_threshold_perc` Float64,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, proposal_id)