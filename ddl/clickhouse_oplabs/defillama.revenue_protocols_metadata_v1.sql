CREATE DATABASE IF NOT EXISTS defillama;

CREATE TABLE IF NOT EXISTS defillama.revenue_protocols_metadata_v1 (
    `dt` Date,
    `name` String,
    `defillamaId` String,
    `displayName` Nullable(String),
    `module` Nullable(String),
    `category` Nullable(String),
    `logo` Nullable(String),
    `chains` Array(Nullable(String)),
    `protocolType` Nullable(String),
    `methodologyURL` Nullable(String),
    `methodology` Array(
        Tuple(
            key Nullable(String),
            value Nullable(String)
        )
    ),
    `latestFetchIsOk` Nullable(Bool),
    `slug` Nullable(String),
    `id` Nullable(String),
    `parentProtocol` Nullable(String)
) ENGINE = ReplacingMergeTree(dt)
ORDER BY
    (dt, name, defillamaId)