CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt`                      Date,
    `proposal_id`             String,
    `start_block_ts`          DateTime,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, proposal_id)