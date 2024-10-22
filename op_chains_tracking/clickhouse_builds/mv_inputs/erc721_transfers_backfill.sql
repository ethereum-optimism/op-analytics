INSERT INTO {view_name}
SELECT
    transaction_hash,
    block_timestamp,
    block_number,
    log_index,
    address AS token_contract_address,
    reinterpretAsUInt256(reverse(unhex(splitByChar(',', topics)[4]))) token_id,
    '0x' || lower(substring(splitByChar(',', topics)[2], 27, 40)) AS transfer_from_address,
    '0x' || lower(substring(splitByChar(',', topics)[3], 27, 40)) AS transfer_to_address,
    chain_id, chain, network, insert_time
FROM {chain}_logs
WHERE splitByChar(',', topics)[1] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND splitByChar(',', topics)[4] != '' --is an nft transfer
    AND block_timestamp BETWEEN '{start_date}' AND '{end_date}'
    and is_deleted = 0