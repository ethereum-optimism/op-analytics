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
    `trace_address`,
    `from_address`,
    `to_address`,
    CAST(amount_lossless AS UInt256) AS `amount`,
    `amount_lossless`,
    `input_method_id`,
    `call_type`,
    `transfer_type`
FROM INPUT_BLOCKBATCH('blockbatch/native_transfers/native_transfers_v1')
WHERE amount_lossless IS NOT NULL 