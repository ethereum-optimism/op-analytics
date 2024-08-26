INSERT INTO {table_name}

SELECT
    chain,
    network,
    chain_id,
    toDate(block_timestamp) AS dt,
    toFixedString(ifNull(to_address, repeat('0', 42)), 42) AS to_address,

    countState(*) AS count_transactions_to,
    countIf(receipt_status = 1) AS count_transactions_to_success,

    uniqState(t.block_number) AS count_blocks_to,
    uniqStateIf(t.block_number, receipt_status = 1) AS count_blocks_to_success,

    sum(receipt_gas_used) AS sum_gas_used_to,
    sumIf(receipt_gas_used, receipt_status = 1) AS sum_gas_used_to_success,

    uniqState(toFixedString(ifNull(from_address, repeat('0', 42)), 42)) AS count_unique_from_addresses_to,
    uniqStateIf(toFixedString(ifNull(from_address, repeat('0', 42)), 42), receipt_status = 1) AS count_unique_from_addresses_to_success,

    SUM(gas_fee_eth) AS sum_eth_gas_fees_to,
    sumIf(gas_fee_eth, receipt_status = 1) AS sum_eth_gas_fees_to_success,

    SUM(input_byte_length) AS sum_input_bytes_to,
    sumIf(input_byte_length, receipt_status = 1) AS sum_input_bytes_to_success,

    SUM(l1_contrib_l2_eth_fees) AS sum_l1_fee_contrib_eth_gas_fees_to,
    sumIf(l1_contrib_l2_eth_fees, receipt_status = 1) AS sum_l1_fee_contrib_eth_gas_fees_to_success,

    SUM(l2_contrib_l2_eth_fees) AS sum_l2_fee_contrib_eth_gas_fees_to,
    sumIf(l2_contrib_l2_eth_fees, receipt_status = 1) AS sum_l2_fee_contrib_eth_gas_fees_to_success,

    SUM(l2_contrib_l2_eth_fees_base_fee) AS sum_l2_base_contrib_eth_gas_fees_to,
    sumIf(l2_contrib_l2_eth_fees_base_fee, receipt_status = 1) AS sum_l2_base_contrib_eth_gas_fees_to_success,
    SUM(l2_contrib_l2_eth_fees_priority_fee) AS sum_l2_priority_contrib_eth_gas_fees_to,
    sumIf(l2_contrib_l2_eth_fees_priority_fee, receipt_status = 1) AS sum_l2_priority_contrib_eth_gas_fees_to_success,

    SUM((t.receipt_gas_used * t.gas_price)/ 1e9)/ SUM(t.receipt_gas_used) AS avg_l2_gas_price_gwei,
    SUM(CAST(receipt_gas_used AS Float64) * l2_base_fee_gwei)/ SUM(t.receipt_gas_used) AS avg_l2_base_fee_gas_price_gwei,
    SUM(CAST(receipt_gas_used AS Float64) * l2_priority_fee_gwei)/ SUM(t.receipt_gas_used) AS avg_l2_priority_fee_gas_price_gwei,
    
    SUM(COALESCE(receipt_l1_gas_used, input_byte_length) * receipt_l1_gas_price/1e9)/ SUM(COALESCE(receipt_l1_gas_used,input_byte_length)) AS avg_l1_gas_price_gwei


FROM {chain}_transactions_unique_mv t
    WHERE gas_price > 0
    AND is_deleted = 0
    AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
    1,2,3,4,5

SETTINGS max_execution_time = 3000