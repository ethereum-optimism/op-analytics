-- Since this table is meant to be exported we use BigQuery-compatible data types.

CREATE TABLE IF NOT EXISTS _placeholder_
(
    `dt` Date,
    `chain` String,
    `chain_id` Int32,
    `network` String,
    `block_timestamp` DateTime,
    `block_number` Int64,
    `transaction_hash` String,
    `transaction_index` Int64,
    `tr_from_address` String,
    `tx_from_address` String,
    `contract_address`String,
    `tx_to_address` String,
    `trace_address` String,
    `trace_type` String,
    `gas` String,
    `gas_used` String,
    `value` UInt256,
    `code` String,
    `call_type` String,
    `reward_type` String,
    `subtraces` Int64,
    `error` String,
    `status` Int64,
    `tx_method_id` String,
    `code_bytelength` Int64,
    `is_erc7802` Bool,
    `has_oft_events` Bool,
    `has_ntt_events` Bool,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
ENGINE = ReplacingMergeTree
ORDER BY (dt, chain, chain_id, network, block_number, transaction_hash, transaction_index, trace_address)
