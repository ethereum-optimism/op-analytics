SELECT
    l.network_id
    ,l.chain_id
    ,l.chain
    ,l.dt
    ,l.block_timestamp
    ,l.block_number
    ,l.block_hash
    ,l.transaction_hash
    ,l.transaction_index
    ,l.log_index
    ,l.address AS contract_address
    ,l.data.hex_to_lossy() AS amount
    ,l.data.hex_to_lossless() AS amount_lossless
    ,indexed_args.list[0][0].indexed_event_arg_to_address() AS from_address
    ,indexed_args.list[1][0].indexed_event_arg_to_address() AS to_address
    ,CASE WHEN array_length(indexed_args.list) > 2 THEN indexed_args.list[2][0].hex_to_lossy() END AS token_id
FROM {{ raw_logs }} AS l
WHERE
    true
    AND l.topic0 LIKE '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef%' -- transfers
