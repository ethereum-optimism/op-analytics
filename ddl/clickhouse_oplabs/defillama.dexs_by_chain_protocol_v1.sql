CREATE DATABASE IF NOT EXISTS defillama;

CREATE TABLE IF NOT EXISTS defillama.dexs_by_chain_protocol_v1 (
    `version` DateTime DEFAULT now(),
    `dt` Date,
    `chain` String,
    `protocol` String,
    `total_volume_usd` Nullable(Int64),
    `total_fees_usd` Nullable(Int64),
    `total_revenue_usd` Nullable(Int64)
) ENGINE = ReplacingMergeTree(version)
ORDER BY
    (dt, chain)