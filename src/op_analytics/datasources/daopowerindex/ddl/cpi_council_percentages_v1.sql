CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `council_name` String,
    `original_percentage` Float64,
    `redistributed_percentage` Float64
)
ENGINE = ReplacingMergeTree
ORDER BY (dt)