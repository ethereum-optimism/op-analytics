CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `hhi` Float64,
    `cpi` Float64
)
ENGINE = ReplacingMergeTree
ORDER BY (dt)