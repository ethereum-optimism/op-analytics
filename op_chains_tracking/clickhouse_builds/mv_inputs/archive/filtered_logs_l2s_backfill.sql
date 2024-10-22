INSERT INTO {table_name}

WITH block_ranges AS (
SELECT min(number) AS min_num, max(number) AS max_num
    from {chain}_blocks b2
    where b2.timestamp between toDate('{start_date}') and toDate('{end_date}')
)

SELECT
transaction_hash as transaction_hash
, chain as chain_name
, block_timestamp AS block_timestamp
, block_number AS block_number
, l.insert_time AS insert_time

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
