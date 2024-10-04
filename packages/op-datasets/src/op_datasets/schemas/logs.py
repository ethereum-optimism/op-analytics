"""

CREATE TABLE default.base_logs
(
    `id` String,
    `block_number` UInt64 CODEC(Delta(8), ZSTD(1)),
    `block_hash` FixedString(66),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `log_index` UInt64,
    `address` FixedString(42),
    `data` String,
    `topics` String,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `chain` String,
    `network` String,
    `chain_id` Int32,
    `insert_time` DateTime,
    `is_deleted` UInt8,
    `topic0` String MATERIALIZED splitByChar(',', topics)[1],
    INDEX block_timestamp_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX address_idx address TYPE bloom_filter GRANULARITY 1,
    INDEX topics0_idx topic0 TYPE bloom_filter GRANULARITY 1
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', insert_time, is_deleted)
ORDER BY (block_number, id)
SETTINGS index_granularity = 8192, min_age_to_force_merge_seconds = 1200, min_age_to_force_merge_on_partition_only = true
"""
