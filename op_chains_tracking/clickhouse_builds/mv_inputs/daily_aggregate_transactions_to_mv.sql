CREATE MATERIALIZED VIEW {view_name}
TO {table_name}

AS SELECT
    chain,
    network,
    chain_id,
    toDate(block_timestamp) AS dt,
    toFixedString(ifNull(to_address, repeat('0', 42)), 42) AS to_address,

    countState(*) AS count_transactions_to,
    countStateIf(receipt_status = 1) AS count_transactions_to_success,

    uniqState(t.block_number) AS count_blocks_to,
    uniqStateIf(t.block_number, receipt_status = 1) AS count_blocks_to_success,

    sumState(CAST(receipt_gas_used AS UInt128)) AS sum_gas_used_to,
    sumStateIf(CAST(receipt_gas_used AS UInt128), receipt_status = 1) AS sum_gas_used_to_success,

    uniqState(toFixedString(ifNull(from_address, repeat('0', 42)), 42)) AS count_unique_from_addresses_to,
    uniqStateIf(toFixedString(ifNull(from_address, repeat('0', 42)), 42), receipt_status = 1) AS count_unique_from_addresses_to_success,

    sumState(CAST(gas_fee_eth AS Float64)) AS sum_eth_gas_fees_to,
    sumStateIf(CAST(gas_fee_eth AS Float64), receipt_status = 1) AS sum_eth_gas_fees_to_success,
    sumState(CAST(l1_contrib_l2_eth_fees AS Float64)) AS sum_l1_fee_contrib_eth_gas_fees_to,
    sumStateIf(CAST(l1_contrib_l2_eth_fees AS Float64), receipt_status = 1) AS sum_l1_fee_contrib_eth_gas_fees_to_success,
    sumState(CAST(l2_contrib_l2_eth_fees AS Float64)) AS sum_l2_fee_contrib_eth_gas_fees_to,
    sumStateIf(CAST(l2_contrib_l2_eth_fees AS Float64), receipt_status = 1) AS sum_l2_fee_contrib_eth_gas_fees_to_success,
    -- sumState(CAST(l2_contrib_l2_eth_fees_base_fee AS Float64)) AS sum_l2_base_contrib_eth_gas_fees_to,
    -- sumStateIf(CAST(l2_contrib_l2_eth_fees_base_fee AS Float64), receipt_status = 1) AS sum_l2_base_contrib_eth_gas_fees_to_success,
    -- sumState(CAST(l2_contrib_l2_eth_fees_priority_fee AS Float64)) AS sum_l2_priority_contrib_eth_gas_fees_to,
    -- sumStateIf(CAST(l2_contrib_l2_eth_fees_priority_fee AS Float64), receipt_status = 1) AS sum_l2_priority_contrib_eth_gas_fees_to_success,

    sumState(CAST(input_byte_length AS Int64)) AS sum_input_bytes_to,
    sumStateIf(CAST(input_byte_length AS Int64), receipt_status = 1) AS sum_input_bytes_to_success,

    avgState(CAST((t.receipt_gas_used * t.gas_price) / 1e9 AS Float64)) AS avg_l2_gas_price_gwei,
    -- avgState(CAST(receipt_gas_used AS Float64) * CAST(l2_base_fee_gwei AS Float64)) AS avg_l2_base_fee_gas_price_gwei,
    -- avgState(CAST(receipt_gas_used AS Float64) * CAST(l2_priority_fee_gwei AS Float64)) AS avg_l2_priority_fee_gas_price_gwei,
    avgState(CAST(COALESCE(receipt_l1_gas_used, input_byte_length) * receipt_l1_gas_price / 1e9 AS Float64)) AS avg_l1_gas_price_gwei

FROM {chain}_transactions_unique_mv t
WHERE gas_price > 0
AND is_deleted = 0

GROUP BY
    chain,
    network,
    chain_id,
    dt,
    to_address