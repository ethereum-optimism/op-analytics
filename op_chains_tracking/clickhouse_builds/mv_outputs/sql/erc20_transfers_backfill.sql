INSERT INTO base_erc20_transfers_mv
SELECT
    transaction_hash,
    block_timestamp,
    block_number,
    log_index,
    address AS token_contract_address,
    reinterpretAsUInt256(reverse(unhex(substring(data, 3)))) AS amount_raw,
    '0x' || lower(substring(splitByChar(',', topics)[2], 27, 40)) AS transfer_from_address,
    '0x' || lower(substring(splitByChar(',', topics)[3], 27, 40)) AS transfer_to_address,
    chain_id, chain, network, insert_time
FROM base_logs
WHERE splitByChar(',', topics)[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND splitByChar(',', topics)[4] = '' --not an nft transfer
    AND block_timestamp BETWEEN '2024-09-11' AND '2024-09-13'
    and is_deleted = 0

SETTINGS max_execution_time = 3000