INSERT INTO {view_name}

select distinct
    t.block_timestamp AS block_timestamp,
    t.block_number AS block_number,
    t.hash AS transaction_hash,
    t.from_address AS from_address,
    t.to_address AS to_address,
    coalesce(t.value/1e18,0) AS tx_value,
    coalesce(t.receipt_l1_fee/1e18,0) AS l1_gas_fee,
    coalesce((t.gas_price * t.receipt_gas_used)/1e18,0) AS l2_gas_fee,
    coalesce(((t.gas_price * t.receipt_gas_used)/1e18),0) + coalesce((t.receipt_l1_fee/1e18),0) AS total_gas_fee,
    t.chain as chain_name

from {chain}_filtered_logs_l2s l
inner join {chain}_transactions t
on t.hash = l.transaction_hash
    and t.block_number = l.block_number
    and t.block_timestamp = l.block_timestamp
    and t.chain = l.chain_name
    AND t.chain_id = l.chain_id

where t.network = 'mainnet'
    and t.receipt_status = 1
    and t.gas_price > 0
    AND t.is_deleted = 0

    -- logs filter
    AND l.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND l.block_number between 
        (SELECT min(number) from {chain}_blocks b2 where b2.timestamp >= toDate('{start_date}') )
                    and             
        (SELECT max(number) from {chain}_blocks b2 where b2.timestamp < toDate('{end_date}'))
    
    -- txs filter
    AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND t.block_number between 
        (SELECT min(number) from {chain}_blocks b2 where b2.timestamp >= toDate('{start_date}') )
                    and             
        (SELECT max(number) from {chain}_blocks b2 where b2.timestamp < toDate('{end_date}'))