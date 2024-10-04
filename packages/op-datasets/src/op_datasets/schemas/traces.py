"""

CREATE TABLE default.base_traces
(
    `id` String,
    `block_number` UInt64 CODEC(Delta(4), ZSTD(1)),
    `block_hash` FixedString(66),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `value` UInt256,
    `input` String,
    `output` String,
    `trace_type` String,
    `call_type` String,
    `reward_type` String,
    `gas` UInt128,
    `gas_used` UInt128,
    `subtraces` UInt256,
    `trace_address` String,
    `error` String,
    `status` UInt8,
    `trace_id` String,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `chain` String,
    `network` String,
    `chain_id` Int32,
    `insert_time` DateTime,
    `is_deleted` UInt8,
    PROJECTION proj_block_timestamp
    (
        SELECT *
        ORDER BY
            block_timestamp,
            id
    )
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', insert_time, is_deleted)
ORDER BY id
SETTINGS index_granularity = 8192
"""
