CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `block_number` UInt64 CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `transaction_index` Int64,
    `tr_from_address` FixedString(42),
    `tx_from_address` FixedString(42),
    `deployment_type` String,
    `contract_address` FixedString(42),
    `trace_address` String,
    `trace_type` String,
    `gas` UInt128,
    `gas_used` UInt128,
    INDEX dt_idx chain TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, block_number, transaction_hash, transaction_index, trace_address)
