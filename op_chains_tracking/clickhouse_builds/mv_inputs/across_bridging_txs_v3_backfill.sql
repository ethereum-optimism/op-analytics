INSERT INTO {table_name}

WITH block_ranges AS (
SELECT min(number) AS min_num, max(number) AS max_num
    from {chain}_blocks
    where timestamp >= toDate('{start_date}')
        and timestamp < toDate('{end_date}')
        and timestamp < toDate(NOW())
)

select
    x.*
    ,c.chain_name as dst_chain
from (
    select
        l.block_timestamp as block_timestamp
        ,l.block_number as block_number
        ,l.chain as src_chain
        ,l.address as contract_address
        ,l.transaction_hash as transaction_hash
        ,reinterpretAsUInt64(reverse(unhex(substring(splitByChar(',', l.topics)[3], 3)))) as deposit_id
        ,'0x' || substring(substring(l.data, 3), 25, 40) as input_token_address
        ,'0x' || substring(substring(l.data, 3), 89, 40) as output_token_address
        ,cast(reinterpretAsUInt64(reverse(unhex(substring(splitByChar(',', l.topics)[2], 3)))) as String) as dst_chain_id
        ,reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 129, 64)))) as input_amount
        ,reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 193, 64)))) as output_amount
        ,reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 257, 64)))) as quote_timestamp
        ,reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 321, 64)))) as fill_deadline
        ,reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 385, 64)))) as exclusivity_deadline
        ,'0x' || right(substring(substring(l.data, 3), 449, 64), 40) as recipient_address
        ,'0x' || right(substring(substring(l.data, 3), 513, 64), 40) as relayer_address
        ,t.from_address as depositor_address
        ,CASE
            WHEN substring(t.input, -10) = '1dc0de0001' THEN 'SuperBridge'
            WHEN substring(t.input, -10) = '1dc0de0002' THEN 'Brid.gg'
            ELSE null
        END AS integrator
        , l.log_index
        , l.insert_time
    from {chain}_logs as l
    join (
            SELECT * FROM {chain}_transactions
            WHERE block_number >= (SELECT min_num FROM block_ranges )
                    and block_number < (SELECT max_num FROM block_ranges)
                AND is_deleted = 0
                AND receipt_status = 1
                AND network = 'mainnet'
                AND gas_price > 0 
            )  as t
        on l.transaction_hash = t.hash
        and l.block_timestamp = t.block_timestamp
        and l.block_number = t.block_number
        and l.chain = t.chain
    join across_bridge_metadata as c
        on l.chain = c.chain_name
    where 1=1
        and splitByChar(',', l.topics)[1] = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        -- and l.network = 'mainnet'
        AND l.block_number between 
        (SELECT min_num FROM block_ranges )
                    and             
        (SELECT max_num FROM block_ranges)
        AND l.is_deleted = 0
        AND t.gas_price > 0 
        AND l.data IS NOT NULL AND l.data != '' -- info is there
        AND l.chain IN (SELECT chain_name FROM across_bridge_metadata)
) as x

join across_bridge_metadata as c
    on x.dst_chain_id = c.mainnet_chain_id
where integrator is not null
AND block_timestamp < toDate(NOW())

SETTINGS max_execution_time = 5000