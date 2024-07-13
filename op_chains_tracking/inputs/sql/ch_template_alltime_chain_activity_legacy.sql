SELECT *
    , num_blocks * block_time_sec as active_secs_per_day
    
    , l2_num_txs_per_day / num_blocks AS l2_num_txs_per_day_per_block
        , l2_num_txs_per_day / (num_blocks*block_time_sec) AS num_user_txs_per_second
        
    , l2_gas_used / num_blocks AS l2_gas_used_per_block
        , l2_gas_used / (num_blocks*block_time_sec) AS l2_gas_used_per_second
        
    , l2_eth_fees_per_day / num_blocks AS l2_eth_fees_per_block
        , l2_eth_fees_per_day / (num_blocks*block_time_sec) AS l2_eth_fees_per_second
    
--     , CASE WHEN dt = MIN(dt) OVER (PARTITION BY chain, network) THEN 1 ELSE 0 END AS is_initial_day
    
    , chain as chain_name --make name align with registry

FROM (
    SELECT
        DATE_TRUNC('day', toDateTime(block_timestamp)) AS dt,
        chain, network, chain_id, cast(@block_time_sec@ as Float64) AS block_time_sec,

        COUNT(*) AS num_raw_txs,
        COUNT(DISTINCT block_number) AS num_blocks,

        SUM(CASE WHEN gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000015' THEN 1 ELSE 0 END) AS l2_num_attr_deposit_txs_per_day,
        SUM(CASE WHEN gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000007' THEN 1 ELSE 0 END) AS l2_num_user_deposit_txs_per_day,
        SUM(CASE WHEN gas_price > 0 THEN 1 ELSE 0 END) AS l2_num_txs_per_day,
        SUM(CASE WHEN receipt_status = 1 AND gas_price > 0 THEN 1 ELSE 0 END) AS l2_num_success_txs_per_day,

        COUNT(DISTINCT from_address) AS num_senders_per_day,

        SUM(t.receipt_gas_used) AS l2_gas_used,
        SUM(cast(receipt_l1_gas_used as Nullable(Int64)) ) AS l1_gas_used_on_l2,
        SUM(cast(receipt_l1_gas_used as Nullable(Float64)) * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar)) AS l1_gas_paid,
        SUM(cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar) AS blob_gas_paid,
        SUM((length(unhex(input)) - 1)) AS calldata_bytes_l2_per_day,

        SUM(CASE WHEN gas_price > 0 THEN cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_fee_scalar ELSE 0 END) AS l1_gas_paid_user_txs,
        SUM(CASE WHEN gas_price > 0 THEN cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar ELSE 0 END) AS blob_gas_paid_user_txs,

        SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used ELSE 0 END) AS l1_gas_used_user_txs_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN (length(unhex(input)) - 1) ELSE 0 END) AS calldata_bytes_user_txs_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN t.receipt_gas_used ELSE 0 END) AS l2_gas_used_user_txs_per_day,

        SUM(CAST(gas_price * t.receipt_gas_used + receipt_l1_fee AS Nullable(Float64)) / 1e18) AS l2_eth_fees_per_day,
        median(
                CASE WHEN gas_price > 0 THEN 
                CAST(gas_price * t.receipt_gas_used + receipt_l1_fee AS Nullable(Float64)) / 1e18
                ELSE NULL END
            ) AS median_l2_eth_fees_per_tx,

        SUM(CAST(receipt_l1_fee AS Nullable(Float64)) / 1e18) AS l1_contrib_l2_eth_fees_per_day,
        SUM(CAST(gas_price * t.receipt_gas_used AS Nullable(Float64)) / 1e18) AS l2_contrib_l2_eth_fees_per_day,

        SUM(cast(receipt_l1_gas_used as Nullable(Float64)) * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar) * cast(receipt_l1_gas_price AS Nullable(Float64)) / 1e18) AS l1_l1gas_contrib_l2_eth_fees_per_day,
        SUM(cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar * cast(receipt_l1_blob_base_fee AS Nullable(Float64)) / 1e18) AS l1_blobgas_contrib_l2_eth_fees_per_day,

        SUM(CAST((gas_price - max_priority_fee_per_gas) * t.receipt_gas_used AS Nullable(Float64)) / 1e18) AS l2_contrib_l2_eth_fees_base_fee_per_day,
        SUM(CAST(max_priority_fee_per_gas * t.receipt_gas_used AS Nullable(Float64)) / 1e18) AS l2_contrib_l2_eth_fees_priority_fee_per_day,

        SUM(
            16 * (length(replace(toString(unhex(input)), '\0', '')) - 1)
            + 4 * ((length(unhex(input)) - 1) - (length(replace(toString(unhex(input)), '\0', '')) - 1))
        ) AS input_calldata_gas_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN
            16 * (length(replace(toString(unhex(input)), '\0', '')) - 1)
            + 4 * ((length(unhex(input)) - 1) - (length(replace(toString(unhex(input)), '\0', '')) - 1))
            ELSE 0 END) AS input_calldata_gas_user_txs_l2_per_day,

        SUM(receipt_l1_gas_used / 16) AS compressedtxsize_approx_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used / 16 ELSE 0 END) AS compressedtxsize_approx_user_txs_l2_per_day,

        SUM((CAST(receipt_l1_gas_price AS Nullable(Float64)) / CAST(1e9 AS Nullable(Float64))) * CAST(receipt_l1_gas_used AS Nullable(Float64))) / SUM(CAST(receipt_l1_gas_used AS Nullable(Float64))) AS avg_l1_gas_price_on_l2,

        SUM((CAST(receipt_l1_blob_base_fee AS Nullable(Float64)) / CAST(1e9 AS Nullable(Float64))) * CAST(receipt_l1_gas_used AS Nullable(Float64))) / SUM(CAST(receipt_l1_gas_used AS Nullable(Float64))) AS avg_blob_base_fee_on_l2,
        
        SUM(CASE WHEN gas_price > 0 THEN CAST(t.receipt_gas_used * gas_price AS Nullable(Float64)) / 1e9 ELSE NULL END)
        / SUM(CASE WHEN gas_price > 0 THEN t.receipt_gas_used ELSE NULL END) AS avg_l2_gas_price, -- if not free
        
        SUM(CASE WHEN gas_price > 0 THEN CAST(t.receipt_gas_used AS Nullable(Float64)) * CAST((gas_price - max_priority_fee_per_gas) AS Nullable(Float64)) / 1e9 ELSE NULL END)
        / SUM(CASE WHEN gas_price > 0 THEN t.receipt_gas_used ELSE NULL END) AS base_fee_gwei, -- if not free
        
        SUM( CASE WHEN gas_price = 0 THEN 0 ELSE CAST(t.receipt_gas_used*receipt_l1_gas_price as Nullable(Float64)) END / 1e18) AS equivalent_l1_tx_fee,
        
        AVG(CASE WHEN gas_price > 0 THEN receipt_l1_fee_scalar ELSE NULL END) AS avg_l1_fee_scalar,
        coalesce( AVG(CASE WHEN gas_price > 0 THEN receipt_l1_blob_base_fee_scalar ELSE NULL END) ,0) AS avg_l1_blob_fee_scalar
        
    FROM @chain_db_name@_transactions t final
    WHERE block_timestamp >= DATE_TRUNC('day',now() - interval '@trailing_days@ days')
    
    GROUP BY 1,2,3,4,5
    ) a

SETTINGS max_execution_time = @max_execution_secs@;