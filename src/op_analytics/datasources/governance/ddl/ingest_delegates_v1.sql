CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `delegate` String,
    `num_of_delegators` Nullable(Int64),
    `direct_vp` Nullable(String),
    `advanced_vp` Nullable(String),
    `voting_power` Nullable(String),
    `contract` Nullable(String),
    INDEX dt_idx dt TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, delegate)
