CREATE DATABASE IF NOT EXISTS intermediate_models;

CREATE TABLE IF NOT EXISTS intermediate_models.create_traces_v1__gcs
(
    `network` String,
    `chain_id` Int32,
    `block_timestamp` UInt32,
    `block_number` Int64,
    `block_hash` String,
    `transaction_hash` String,
    `transaction_index` Int64,
    `tr_from_address` String,
    `tx_from_address` String,
    `contract_address` String,
    `tx_to_address` String,
    `value_64` Int64,
    `value_lossless` String,
    `code` String,
    `output` String,
    `trace_type` String,
    `call_type` String,
    `reward_type` String,
    `gas` Int64,
    `gas_used` Int64,
    `subtraces` Int64,
    `trace_address` String,
    `error` String,
    `status` Int64,
    `tx_method_id` String,
    `code_bytelength` Float64,
) ENGINE = S3(
    'https://storage.googleapis.com/oplabs-tools-data-sink/intermediate/contract_creation/create_traces_v1/chain=*/dt=*/out.parquet',
    'HMAC_KEY',
    'HMAC_VALUE',
    'parquet'
);

CREATE VIEW IF NOT EXISTS intermediate_models.create_traces_v1
AS 
SELECT
    extract(_path, 'chain=(\w+)\/') as chain,
    extract(_path, 'dt=(\d{4}-\d{2}-\d{2})\/') as dt,
    *
FROM intermediate_models.create_traces_v1__gcs
