with x AS (
    SELECT
        l.block_timestamp AS block_timestamp
        ,l.block_number AS block_number
        ,l.chain AS src_chain
        ,l.address AS contract_address
        ,l.transaction_hash AS transaction_hash
        ,parse_reversed_hex(substring(split_string(l.topics, ',')[3], 3))) AS deposit_id
        ,'0x' || substring(substring(l.data, 3), 25, 40) AS input_token_address
        ,'0x' || substring(substring(l.data, 3), 89, 40) AS output_token_address
        ,cast(parse_reversed_hex(substring(string_split(l.topics, ',')[2], 3) AS String) AS dst_chain_id
        ,parse_reversed_hex(substring(substring(l.data, 3), 129, 64)) AS input_amount
        ,parse_reversed_hex(substring(substring(l.data, 3), 193, 64)) AS output_amount
        ,parse_reversed_hex(substring(substring(l.data, 3), 257, 64)) AS quote_timestamp
        ,parse_reversed_hex(substring(substring(l.data, 3), 321, 64)) AS fill_deadline
        ,parse_reversed_hex(substring(substring(l.data, 3), 385, 64)) AS exclusivity_deadline
        ,'0x' || right(substring(substring(l.data, 3), 449, 64), 40) AS recipient_address
        ,'0x' || right(substring(substring(l.data, 3), 513, 64), 40) AS relayer_address
        ,t.from_address AS depositor_address
        ,CASE
            WHEN substring(t.input, -10) = '1dc0de0001' THEN 'SuperBridge'
            WHEN substring(t.input, -10) = '1dc0de0002' THEN 'Brid.gg'
            ELSE null
        END AS integrator
        ,l.log_index AS log_index
        ,l.insert_time AS insert_time
        ,wei_to_eth(CASE WHEN t.gas_price = 0 THEN 0 ELSE t.gas_price * t.receipt_gas_used END) AS l2_fee
        ,wei_to_eth(CASE WHEN t.gas_price = 0 THEN 0 ELSE t.receipt_l1_fee END) AS l1_fee
    FROM {{ raw_logs }} AS l
    join {{ raw_transactions }} AS t
        on l.transaction_hash = t.hash
        and l.block_timestamp = t.block_timestamp
        and l.block_number = t.block_number
        and l.chain = t.chain
    -- join {{ across_bridge_metadata }} AS c
    --     on l.chain = c.chain_name
    --     and l.address = lower(c.spokepool_address)
    where 1=1
        and split_string(l.topics, ',')[1] = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        and l.network = 'mainnet'
        and t.receipt_status = 1
        AND t.is_deleted = 0
        AND l.is_deleted = 0
        AND t.gas_price > 0
        AND l.data IS NOT NULL AND l.data != '' -- info is there
        AND l.chain IN (SELECT chain_name FROM across_bridge_metadata)
        AND l.block_timestamp > '2024-05-01'
)

SELECT
    x.*
    ,case
        when x.dst_chain_id = 1 then 'ethereum'
        else c.chain_name end as dst_chain
    ,x.l2_fee + x.l1_fee AS tx_fee
FROM x
left join {{ op_stack_chain_metadata }} AS c
    on x.dst_chain_id = c.mainnet_chain_id
where x.integrator is not null
