CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `cpi_value` Float64,
    `active_percent` Float64,
    `inactive_percent` Float64
)
ENGINE = ReplacingMergeTree
ORDER BY (dt)