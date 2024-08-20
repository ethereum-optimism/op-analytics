    INSERT INTO default.across_bridge_metadata

    select
        *
        ,CASE chain_name
            WHEN 'base' THEN '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
            WHEN 'ethereum' THEN '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
            WHEN 'lisk' THEN '0x9552a0a6624a23b848060ae5901659cdda1f83f8'
            WHEN 'mode' THEN '0x3bad7ad0728f9917d1bf08af5782dcbd516cdd96'
            WHEN 'op' THEN '0x6f26bf09b1c792e3228e5467807a900a503c0281'
            WHEN 'redstone' THEN '0x13fdac9f9b4777705db45291bbff3c972c6d1d97'
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
