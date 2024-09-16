INSERT INTO {table_name}

SELECT
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

    sumState(CAST(gas_fee_sql AS Float64)) AS sum_eth_gas_fees_to,
    sumStateIf(CAST(gas_fee_sql AS Float64), receipt_status = 1) AS sum_eth_gas_fees_to_success,
    sumState(CAST(l1_contrib_l2_eth_fees AS Float64)) AS sum_l1_fee_contrib_eth_gas_fees_to,
    sumStateIf(CAST(l1_contrib_l2_eth_fees AS Float64), receipt_status = 1) AS sum_l1_fee_contrib_eth_gas_fees_to_success,
    sumState(CAST(l2_contrib_l2_eth_fees AS Float64)) AS sum_l2_fee_contrib_eth_gas_fees_to,
    sumStateIf(CAST(l2_contrib_l2_eth_fees AS Float64), receipt_status = 1) AS sum_l2_fee_contrib_eth_gas_fees_to_success,
    -- sumState(CAST(l2_contrib_l2_eth_fees_base_fee AS Float64)) AS sum_l2_base_contrib_eth_gas_fees_to,
    -- sumStateIf(CAST(l2_contrib_l2_eth_fees_base_fee AS Float64), receipt_status = 1) AS sum_l2_base_contrib_eth_gas_fees_to_success,
    -- sumState(CAST(l2_contrib_l2_eth_fees_priority_fee AS Float64)) AS sum_l2_priority_contrib_eth_gas_fees_to,
    -- sumStateIf(CAST(l2_contrib_l2_eth_fees_priority_fee AS Float64), receipt_status = 1) AS sum_l2_priority_contrib_eth_gas_fees_to_success,

    sumState(CAST(byte_length_sql AS Int64)) AS sum_input_bytes_to,
    sumStateIf(CAST(byte_length_sql AS Int64), receipt_status = 1) AS sum_input_bytes_to_success,

    avgState(CAST((t.receipt_gas_used * t.gas_price_max) / 1e9 AS Float64)) AS avg_l2_gas_price_gwei,
    -- avgState(CAST(receipt_gas_used AS Float64) * CAST(l2_base_fee_gwei AS Float64)) AS avg_l2_base_fee_gas_price_gwei,
    -- avgState(CAST(receipt_gas_used AS Float64) * CAST(l2_priority_fee_gwei AS Float64)) AS avg_l2_priority_fee_gas_price_gwei,
    avgState(CAST(COALESCE(receipt_l1_gas_used, byte_length_sql) * receipt_l1_gas_price_max / 1e9 AS Float64 )) AS avg_l1_gas_price_gwei,
    avgState(CAST(COALESCE(receipt_l1_gas_used, byte_length_sql) * COALESCE(receipt_l1_blob_base_fee_max,0) / 1e9 AS Float64)) AS avg_blob_base_fee_gwei

FROM (
        SELECT
            t.hash, t.block_number, t.network, t.chain, t.chain_id

                    , argMax( (@byte_length_sql@) , t.insert_time) AS byte_length_sql
                    , argMax( if(gas_price > 0, @gas_fee_sql@,0) , t.insert_time) AS gas_fee_sql
                    -- do multiline comment b/c find and replace makes new lines
                    /* , argMax( (@estimated_size_sql@) , t.insert_time) AS estimated_size_sql
                     , argMax( (@calldata_gas_sql@) , t.insert_time) AS calldata_gas_sql */

                    , argMax(
                            if(gas_price > 0, CAST(receipt_l1_fee AS Nullable(Float64)) / 1e18, 0)
                            , t.insert_time) AS l1_contrib_l2_eth_fees
                    , argMax(
                            if(gas_price > 0, CAST(gas_price * t.receipt_gas_used AS Nullable(Float64)) / 1e18, 0) 
                            , t.insert_time) AS l2_contrib_l2_eth_fees

                    , argMax(
                            if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * COALESCE(16*receipt_l1_base_fee_scalar/1e6,receipt_l1_fee_scalar) * cast(receipt_l1_gas_price AS Nullable(Float64))) / 1e18, 0) 
                            , t.insert_time) AS l1_l1gas_contrib_l2_eth_fees
                    , argMax(
                            if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar/1e6 * cast(receipt_l1_blob_base_fee AS Nullable(Float64))) / 1e18, 0) 
                            , t.insert_time) AS l1_blobgas_contrib_l2_eth_fees

                    , argMax(t.block_timestamp, t.insert_time) AS block_timestamp
                    , argMax(t.gas_price, t.insert_time) AS gas_price_max
                    , argMax(t.receipt_gas_used, t.insert_time) AS receipt_gas_used
                    , argMax(t.receipt_l1_gas_used, t.insert_time) AS receipt_l1_gas_used
                    , argMax(t.receipt_l1_base_fee_scalar, t.insert_time) AS receipt_l1_base_fee_scalar_max
                    , argMax(t.receipt_l1_fee_scalar, t.insert_time) AS receipt_l1_fee_scalar_max
                    , argMax(t.receipt_l1_blob_base_fee_scalar, t.insert_time) AS receipt_l1_blob_base_fee_scalar_max
                    -- , argMax(t.input, t.insert_time) AS input
                    , argMax(t.receipt_l1_blob_base_fee, t.insert_time) AS receipt_l1_blob_base_fee_max
                    , argMax(t.receipt_l1_gas_price, t.insert_time) AS receipt_l1_gas_price_max
                    , argMax(t.receipt_l1_fee, t.insert_time) AS receipt_l1_fee_max
                    , argMax(t.from_address, t.insert_time) AS from_address
                    , argMax(t.to_address, t.insert_time) AS to_address

                    , argMax(t.receipt_status, t.insert_time) AS receipt_status
        FROM {chain}_transactions t
        WHERE 1=1
        AND is_deleted = 0
        AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1,2,3,4,5
    ) t
WHERE gas_price_max > 0
GROUP BY
    chain,
    network,
    chain_id,
    dt,
    to_address

SETTINGS max_execution_time = 3000