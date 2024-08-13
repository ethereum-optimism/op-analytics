INSERT INTO default.across_bridging_txs
with across_supported_chains as (
    select
        *
        ,CASE chain_name
            WHEN 'base' THEN '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
            WHEN 'ethereum' THEN '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
            WHEN 'lisk' THEN '0x9552a0a6624a23b848060ae5901659cdda1f83f8'
            WHEN 'mode' THEN '0x3bad7ad0728f9917d1bf08af5782dcbd516cdd96'
            WHEN 'op' THEN '0x6f26bf09b1c792e3228e5467807a900a503c0281'
            WHEN 'redstone' THEN '0x28077b47cd03326de7838926a63699849dd4fa87'
            ELSE NULL
            END
        AS spokepool_address
    from (
        select
            chain_name
            ,display_name
            ,mainnet_chain_id
        from "default"."op_stack_chain_metadata"
        where
            chain_name in (
                'base',
                'lisk',
                'mode',
                'op',
                'redstone'
            )

        union all

        select
            'ethereum' as chain_name
            ,'Ethereum' as display_name
            ,'1' as mainnet_chain_id
    )
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
        ,reinterpretAsUInt64(reverse(unhex(substring(substring(l.data, 3), 129, 64)))) as input_amount
        ,reinterpretAsUInt64(reverse(unhex(substring(substring(l.data, 3), 193, 64)))) as output_amount
        ,reinterpretAsUInt64(reverse(unhex(substring(substring(l.data, 3), 257, 64)))) as quote_timestamp
        ,reinterpretAsUInt64(reverse(unhex(substring(substring(l.data, 3), 321, 64)))) as fill_deadline
        ,reinterpretAsUInt64(reverse(unhex(substring(substring(l.data, 3), 385, 64)))) as exclusivity_deadline
        ,'0x' || right(substring(substring(l.data, 3), 449, 64), 40) as recipient_address
        ,'0x' || right(substring(substring(l.data, 3), 513, 64), 40) as relayer_address
        ,t.from_address as depositor_address
        ,CASE
            WHEN substring(t.input, -10) = '1dc0de0001' THEN 'SuperBridge'
            WHEN substring(t.input, -10) = '1dc0de0002' THEN 'Brid.gg'
            ELSE null
        END AS integrator
    from "default"."superchain_logs" as l
    join across_supported_chains as c
        on l.chain = c.chain_name
    join "default"."superchain_transactions" as t
        on l.transaction_hash = t.hash
        and l.block_number = t.block_number
        and l.chain = t.chain
    where
        l.block_timestamp >= date_trunc('day', now()) - interval '180' day
        and splitByChar(',', l.topics)[1] = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
        and l.network = 'mainnet'
        and t.receipt_status = 1
) as x
join across_supported_chains as c
    on x.dst_chain_id = c.mainnet_chain_id
where integrator is not null

SETTINGS max_execution_time = 5000
