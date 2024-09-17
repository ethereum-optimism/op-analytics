INSERT INTO {table_name}

WITH block_ranges AS (
SELECT min(number) AS min_num, max(number) AS max_num
    from {chain}_blocks b2
    where 
        ( b2.timestamp between toDate('{start_date}') and toDate('{end_date}') )
        and ( b2.timestamp < toDate(NOW()) )
)

select
    t.block_timestamp AS block_timestamp,
    t.block_number AS block_number,
    t.hash AS transaction_hash,
    t.from_address AS from_address,
    t.to_address AS to_address,
    coalesce(t.value/1e18,0) AS tx_value,
    coalesce(t.receipt_l1_fee/1e18,0) AS l1_gas_fee,
    coalesce((t.gas_price * t.receipt_gas_used)/1e18,0) AS l2_gas_fee,
    coalesce(((t.gas_price * t.receipt_gas_used)/1e18),0) + coalesce((t.receipt_l1_fee/1e18),0) AS total_gas_fee,
    t.chain as chain_name,
    MAX(l.insert_time) AS insert_time_max

from ( --filtered_logs_l2s
            SELECT
            transaction_hash as transaction_hash
            , chain as chain_name
            , chain_id AS chain_id
            , block_timestamp AS block_timestamp
            , block_number AS block_number
            , MAX(l.insert_time) AS insert_time

            FROM {chain}_logs l
                INNER JOIN op_stack_chain_metadata cm
                ON l.chain = cm.oplabs_db_schema 

            WHERE network = 'mainnet'
            AND cm.chain_layer = 'L2'
            AND is_deleted = 0
            AND arrayElement(splitByString(',', topics), 1)
            NOT IN ( '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'
            , '0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'
            , '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
            , '0x9e2c3f17bb468be8e272ad2ccf2b08c8532c2b08e0c378fbd5303ea8b660aa2f' )

            AND l.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
            AND l.block_number between 
                (SELECT min_num FROM block_ranges )
                            and             
                (SELECT max_num FROM block_ranges)
            GROUP BY 1,2,3,4,5
        ) l
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
    
    -- txs filter
    AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
    AND t.block_timestamp < toDate(NOW())
    AND t.block_number between 
        (SELECT min_num FROM block_ranges )
                    and             
        (SELECT max_num FROM block_ranges)
    AND t.block_timestamp < toDate(NOW())

GROUP BY 1,2,3,4,5,6,7,8,9,10

SETTINGS max_execution_time = 5000