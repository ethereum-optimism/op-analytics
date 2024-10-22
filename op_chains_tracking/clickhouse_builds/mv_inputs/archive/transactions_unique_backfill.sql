INSERT INTO {view_name}

-- Native Transactions
SELECT
        t.*
        , if(gas_price > 0, @gas_fee_sql@/1e18, 0) AS gas_fee_eth

        , @byte_length_sql@ AS input_byte_length
        , @num_zero_bytes_sql@ AS input_num_zero_bytes
        , @num_nonzero_bytes_sql@ AS input_num_nonzero_bytes
        , @estimated_size_sql@ AS estimated_size

        , if(gas_price > 0, CAST(receipt_l1_fee AS Nullable(Float64)) / 1e18, 0) AS l1_contrib_l2_eth_fees_per_day
        , if(gas_price > 0, CAST(gas_price * t.receipt_gas_used AS Nullable(Float64)) / 1e18, 0) AS l2_contrib_l2_eth_fees_per_day

        , if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * COALESCE(16*receipt_l1_base_fee_scalar/1e6,receipt_l1_fee_scalar) * cast(receipt_l1_gas_price AS Nullable(Float64))) / 1e18, 0) AS l1_l1gas_contrib_l2_eth_fees_per_day
        , if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar/1e6 * cast(receipt_l1_blob_base_fee AS Nullable(Float64))) / 1e18, 0) AS l1_blobgas_contrib_l2_eth_fees_per_day

FROM {chain}_transactions t

WHERE t.is_deleted = 0
AND t.block_timestamp BETWEEN '{start_date}' AND '{end_date}'

SETTINGS max_execution_time = 3000