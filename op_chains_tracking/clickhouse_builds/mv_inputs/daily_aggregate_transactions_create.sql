CREATE TABLE {view_name}
(
    chain String,
    network String,
    chain_id Int32,
    dt Date,
    to_address String,
    count_transactions_to UInt64,
    count_transactions_to_success UInt64,
    count_blocks_to AggregateFunction(uniq, UInt64),
    count_blocks_to_success AggregateFunction(uniq, UInt64),
    sum_gas_used_to UInt64,
    sum_gas_used_to_success UInt64,

    unique_from_addresses_to AggregateFunction(uniq, FixedString(42)),
    unique_from_addresses_to_success AggregateFunction(uniq, FixedString(42)),

    sum_eth_gas_fees_to Decimal(38,18),
    sum_eth_gas_fees_to_success Decimal(38,18),
    sum_input_bytes_to UInt64,
    sum_input_bytes_to_success UInt64,
    sum_l1_fee_contrib_eth_gas_fees_to Decimal(38,18),
    sum_l1_fee_contrib_eth_gas_fees_to_success Decimal(38,18),
    sum_l2_fee_contrib_eth_gas_fees_to Decimal(38,18),
    sum_l2_fee_contrib_eth_gas_fees_to_success Decimal(38,18),
    sum_l2_base_contrib_eth_gas_fees_to Decimal(38,18),
    sum_l2_base_contrib_eth_gas_fees_to_success Decimal(38,18),
    sum_l2_priority_contrib_eth_gas_fees_to Decimal(38,18),
    sum_l2_priority_contrib_eth_gas_fees_to_success Decimal(38,18),

    avg_l2_gas_price_gwei Decimal(38,9),
    avg_l2_base_fee_gas_price_gwei Decimal(38,9),
    avg_l2_priority_fee_gas_price_gwei Decimal(38,9),
        
    avg_l1_gas_price_gwei Decimal(38,9)

)

ENGINE = AggregatingMergeTree()
ORDER BY (chain, network, chain_id, dt, to_address)