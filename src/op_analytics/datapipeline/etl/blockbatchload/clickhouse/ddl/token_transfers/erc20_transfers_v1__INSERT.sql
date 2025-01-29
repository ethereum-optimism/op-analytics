INSERT INTO blockbatch.token_transfers__erc20_transfers_v1
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
    CAST(amount_lossless AS UInt256) AS `amount`, 
    `from_address`,
    `to_address`
FROM (
    {subquery}
)
