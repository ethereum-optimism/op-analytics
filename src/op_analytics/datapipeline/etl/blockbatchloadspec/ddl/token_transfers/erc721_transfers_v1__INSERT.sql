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
    `log_index`,
    `contract_address`,
    `from_address`,
    `to_address`,
    `token_id`
FROM gcs__blockbatch.token_transfers__erc721_transfers_v1
