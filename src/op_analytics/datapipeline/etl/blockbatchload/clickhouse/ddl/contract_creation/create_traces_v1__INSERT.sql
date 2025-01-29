INSERT INTO blockbatch.contract_creation__create_traces_v1
SELECT
    `chain`,
    `dt`,
    `network`,
    `chain_id`,
    `block_timestamp`,
    `block_number`,
    `block_hash`,
    `transaction_hash`,
    `transaction_index`,
    `tr_from_address`,
    `tx_from_address`,
    `contract_address`,
    `tx_to_address`,
    CAST(value_lossless AS UInt256) AS `value`, 
    `code`,
    `output`,
    `trace_type`,
    `call_type`,
    `reward_type`,
    `gas`,
    `gas_used`,
    `subtraces`,
    `trace_address`,
    `error`,
    `status`,
    `tx_method_id`,
    `code_bytelength`
FROM (
    {subquery}
)
