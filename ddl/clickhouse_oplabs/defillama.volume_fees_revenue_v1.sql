CREATE DATABASE IF NOT EXISTS defillama;

CREATE TABLE IF NOT EXISTS defillama.volume_fees_revenue_v1 (
    `version` DateTime DEFAULT now(),
    `dt` Date,
    `chain` String,
    `total_volume_usd` Nullable(Int64),
    `total_fees_usd` Nullable(Int64),
    `total_revenue_usd` Nullable(Int64)
) ENGINE = ReplacingMergeTree(version)
ORDER BY
    (dt, chain)