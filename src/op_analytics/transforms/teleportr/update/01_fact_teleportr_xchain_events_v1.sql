WITH raw_events AS (
    select
        l.block_timestamp as block_timestamp
        ,l.block_number as block_number
        ,l.chain as src_chain
        ,l.chain_id as src_chain_id
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
        ,l.log_index AS log_index
        ,(t.gas_price * t.gas_used) / 1e18 as l2_fee_eth
        ,(t.receipt_l1_fee) / 1e18 as l1_fee_eth
        ,(t.gas_price * t.gas_used) / 1e18 + (t.receipt_l1_fee) / 1e18 as total_fee_eth
    FROM
        blockbatch_gcs.read_date(
        rootpath = 'ingestion/logs_v1'
        , chain = '*'
        , dt = { dtparam: Date }
        ) as l
    JOIN blockbatch_gcs.read_date(
        rootpath = 'ingestion/transactions_v1'
        , chain = '*'
        , dt = { dtparam: Date }
        ) as t
        ON l.transaction_hash = t.hash
        AND l.block_number = t.block_number
        AND l.chain = t.chain
    JOIN across_bridge_metadata AS c
        ON l.chain = c.chain_name
        AND l.address = c.spokepool_address
    where TRUE
        and splitByChar(',', l.topics)[1] = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        AND t.gas_price > 0
        AND l.data IS NOT NULL AND l.data != '' -- info is there
        -- AND l.block_timestamp > '2024-05-01'
)
,teleportr_xchain_transfers AS (
select
    x.block_timestamp
    ,x.block_number
    ,x.src_chain
    ,x.src_chain_id
    ,c.chain_name as dst_chain
    ,x.dst_chain_id
    ,x.contract_address
    ,x.transaction_hash
    ,x.deposit_id
    ,x.input_token_address
    ,mi.symbol as input_token_symbol
    ,x.input_amount as input_amount_raw
    ,x.input_amount / 10 ^ mi.decimals as input_amount
    ,x.output_token_address
    ,mo.symbol as output_token_symbol
    ,x.output_amount as output_amount_raw
    ,x.output_amount / 10 ^ mo.decimals as output_amount
    ,x.quote_timestamp
    ,x.fill_deadline
    ,x.exclusivity_deadline
    ,x.recipient_address
    ,x.relayer_address
    ,x.depositor_address
    ,x.integrator
    ,x.l2_fee_eth
    ,x.l1_fee_eth
    ,x.total_fee_eth
    ,x.log_index
from raw_events as x
left join op_stack_chain_metadata as c
    on x.dst_chain_id = c.mainnet_chain_id
left join chainsmeta.dim_erc20_token_metadata_v1 as mi
    on x.input_token_address = mi.contract_address
    and x.src_chain_id = mi.chain_id
left join chainsmeta.dim_erc20_token_metadata_v1 as mo
    on x.output_token_address = mo.contract_address
    and x.dst_chain_id = mo.chain_id
where integrator is not null
