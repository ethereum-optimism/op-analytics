INSERT INTO {table_name}

SELECT
    chain,
    network,
    chain_id,
    toDate(block_timestamp) AS dt,
    toFixedString(ifNull(to_address, repeat('0', 42)), 42) AS to_address,

    count(*) AS count_transactions_to,
    countIf(receipt_status = 1) AS count_transactions_to_success,

    uniqState(t.block_number) AS count_blocks_to,
    uniqStateIf(t.block_number, receipt_status = 1) AS count_blocks_to_success,

    sum(gas_used) AS sum_gas_used_to,
    sumIf(gas_used, receipt_status = 1) AS sum_gas_used_to_success,

    uniqState(toFixedString(ifNull(from_address, repeat('0', 42)), 42)) AS count_unique_from_addresses_to,
    uniqStateIf(toFixedString(ifNull(from_address, repeat('0', 42)), 42), receipt_status = 1) AS count_unique_from_addresses_to_success,

    SUM(@gas_fee_sql@/1e18 ) AS sum_eth_gas_fees_to,
    sumIf(@gas_fee_sql@/1e18 , receipt_status = 1) AS sum_eth_gas_fees_to_success,

    SUM(@byte_length_sql@) AS sum_input_bytes_to,
    sumIf(@byte_length_sql@, receipt_status = 1) AS sum_input_bytes_to_success,

        SUM(receipt_l1_fee/1e18) AS sum_l1_fee_contrib_eth_gas_fees_to,
        sumIf(receipt_l1_fee/1e18, receipt_status = 1) AS sum_l1_fee_contrib_eth_gas_fees_to_success,

        SUM(gas_price/1e9*gas_used/1e9)  AS sum_l2_fee_contrib_eth_gas_fees_to,
        sumIf(gas_price/1e9*gas_used/1e9, receipt_status = 1)  AS sum_l2_fee_contrib_eth_gas_fees_to_success,

                SUM(base_fee_per_gas/1e9*gas_used/1e9)  AS sum_l2_base_contrib_eth_gas_fees_to,
                sumIf(base_fee_per_gas/1e9*gas_used/1e9, receipt_status = 1)  AS sum_l2_base_contrib_eth_gas_fees_to_success,
                SUM((gas_price-base_fee_per_gas)/1e9*gas_used/1e9)  AS sum_l2_priority_contrib_eth_gas_fees_to,
                sumIf((gas_price-base_fee_per_gas)/1e9*gas_used/1e9, receipt_status = 1)  AS sum_l2_priority_contrib_eth_gas_fees_to_success,

        SUM((t.receipt_gas_used * t.gas_price)/ 1e9)/ SUM(t.receipt_gas_used) AS avg_l2_gas_price_gwei,
                SUM((t.receipt_gas_used * b.base_fee_per_gas)/ 1e9)/ SUM(t.receipt_gas_used) AS avg_l2_base_fee_gas_price_gwei,
                SUM((t.receipt_gas_used * (t.gas_price-b.base_fee_per_gas))/ 1e9)/ SUM(t.receipt_gas_used) AS avg_l2_priority_fee_gas_price_gwei,
        
        SUM((COALESCE(receipt_l1_gas_used, @byte_length_sql@) * t.gas_price)/ 1e9)/ SUM(COALESCE(receipt_l1_gas_used,@byte_length_sql@)) AS avg_l1_gas_price_gwei


FROM {chain}_transactions t 
INNER JOIN {chain}_blocks b final
            ON t.block_number = b.number 
            AND t.block_timestamp = b.timestamp
    WHERE gas_price > 0
    AND is_deleted = 0
    AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
    1,2,3,4,5

SETTINGS max_execution_time = 3000