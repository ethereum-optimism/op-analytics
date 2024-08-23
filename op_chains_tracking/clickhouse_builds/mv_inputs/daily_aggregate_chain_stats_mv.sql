CREATE MATERIALIZED VIEW {view_name}
TO {table_name}
AS SELECT
    DATE_TRUNC('day', toDateTime(t.block_timestamp)) AS dt,
    chain,
    network,
    chain_id,
    cast({block_time_sec} as Float64) AS block_time_sec,

    countState() AS num_raw_txs,
    uniqExactState(t.block_number) AS num_blocks,

    sumState(if(gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000015', 1, 0)) AS l2_num_attr_deposit_txs_per_day,
    sumState(if(gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000007', 1, 0)) AS l2_num_user_deposit_txs_per_day,
    sumState(if(gas_price > 0, 1, 0)) AS l2_num_txs_per_day,
    sumState(if(receipt_status = 1 AND gas_price > 0, 1, 0)) AS l2_num_success_txs_per_day,

    uniqExactState(from_address) AS num_senders_per_day,

    sumState(t.receipt_gas_used) AS l2_gas_used,
    sumState(cast(receipt_l1_gas_used as Nullable(Int64))) AS l1_gas_used_on_l2,
    sumState(cast(receipt_l1_gas_used as Nullable(Float64)) * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar)) AS l1_gas_paid,
    sumState(cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar) AS blob_gas_paid,
    sumState(@byte_length_sql@) AS calldata_bytes_l2_per_day,

    sumState(if(gas_price > 0, @estimated_size_sql@, 0)) AS estimated_size_user_txs,

    sumState(@estimated_size_sql@ * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar)) AS l1_gas_paid_fjord,
    sumState(@estimated_size_sql@ * receipt_l1_blob_base_fee_scalar) AS blob_gas_paid_fjord,

    sumState(if(gas_price > 0, cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_fee_scalar, 0)) AS l1_gas_paid_user_txs,
    sumState(if(gas_price > 0, cast(receipt_l1_gas_used as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar, 0)) AS blob_gas_paid_user_txs,

    sumState(if(gas_price > 0, receipt_l1_gas_used, 0)) AS l1_gas_used_user_txs_l2_per_day,
    sumState(if(gas_price > 0, @byte_length_sql@, 0)) AS calldata_bytes_user_txs_l2_per_day,
    sumState(if(gas_price > 0, t.receipt_gas_used, 0)) AS l2_gas_used_user_txs_per_day,

    sumState(COALESCE(@gas_fee_sql@ / 1e18,0)) AS l2_eth_fees_per_day,
    medianState(if(gas_price > 0, @gas_fee_sql@ / 1e18, NULL)) AS median_l2_eth_fees_per_tx,

    sumState(if(gas_price > 0, CAST(receipt_l1_fee AS Nullable(Float64)) / 1e18, 0)) AS l1_contrib_l2_eth_fees_per_day,
    sumState(if(gas_price > 0, CAST(gas_price * t.receipt_gas_used AS Nullable(Float64)) / 1e18, 0)) AS l2_contrib_l2_eth_fees_per_day,

    sumState(if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * COALESCE(16*receipt_l1_base_fee_scalar/1e6,receipt_l1_fee_scalar) * cast(receipt_l1_gas_price AS Nullable(Float64))) / 1e18, 0)) AS l1_l1gas_contrib_l2_eth_fees_per_day,
    sumState(if(gas_price > 0, (cast(@estimated_size_sql@ as Nullable(Float64)) * receipt_l1_blob_base_fee_scalar/1e6 * cast(receipt_l1_blob_base_fee AS Nullable(Float64))) / 1e18, 0)) AS l1_blobgas_contrib_l2_eth_fees_per_day,

    sumState(if(gas_price > 0, CAST((base_fee_per_gas) * t.receipt_gas_used AS Nullable(Float64)) / 1e18, 0)) AS l2_contrib_l2_eth_fees_base_fee_per_day,
    sumState(if(gas_price > 0, CAST((gas_price - base_fee_per_gas) * t.receipt_gas_used AS Nullable(Float64)) / 1e18, 0)) AS l2_contrib_l2_eth_fees_priority_fee_per_day,

    sumState(@calldata_gas_sql@) AS input_calldata_gas_l2_per_day,
    sumState(if(gas_price > 0, @calldata_gas_sql@, 0)) AS input_calldata_gas_user_txs_l2_per_day,

    sumState(receipt_l1_gas_used / 16) AS compressedtxsize_approx_l2_per_day_ecotone,
    sumState(if(gas_price > 0, receipt_l1_gas_used / 16, 0)) AS compressedtxsize_approx_user_txs_l2_per_day_ecotone,

    avgWeightedState((CAST(receipt_l1_gas_price AS Nullable(Float64)) / CAST(1e9 AS Nullable(Float64))), CAST(receipt_l1_gas_used AS Nullable(Float64))) AS avg_l1_gas_price_on_l2_state,
    avgWeightedState((CAST(receipt_l1_blob_base_fee AS Nullable(Float64)) / CAST(1e9 AS Nullable(Float64))), CAST(receipt_l1_gas_used AS Nullable(Float64))) AS avg_blob_base_fee_on_l2_state,
    
    avgWeightedState(if(gas_price > 0, CAST(gas_price AS Nullable(Float64)) / 1e9, NULL), if(gas_price > 0, t.receipt_gas_used, NULL)) AS avg_l2_gas_price_state,
    
    avgWeightedState(if(gas_price > 0, CAST((base_fee_per_gas) AS Nullable(Float64)) / 1e9, NULL), if(gas_price > 0, t.receipt_gas_used, NULL)) AS base_fee_gwei_state,
    
    sumState(if(gas_price = 0, 0, CAST(t.receipt_gas_used*receipt_l1_gas_price as Nullable(Float64)) / 1e18)) AS equivalent_l1_tx_fee,
    
    avgState(if(gas_price > 0, COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar), NULL)) AS avg_l1_fee_scalar_state,
    avgState(if(gas_price > 0, receipt_l1_blob_base_fee_scalar, NULL)) AS avg_l1_blob_fee_scalar_state

FROM {chain}_transactions t
INNER JOIN {chain}_blocks b
    ON t.block_number = b.number 
    AND t.block_timestamp = b.timestamp
WHERE
    t.block_timestamp >= DATE_TRUNC('day', now() - interval '30 days')
    AND b.timestamp >= DATE_TRUNC('day', now() - interval '30 days')
    AND t.is_deleted = 0
    AND b.is_deleted = 0
GROUP BY dt, chain, network, chain_id, block_time_sec;