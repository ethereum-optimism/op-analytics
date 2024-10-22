WITH chain_data AS (
  SELECT
        DATE_TRUNC(block_timestamp, DAY) AS dt,
        chain,
        network,
        gas_token_symbol,
        COUNT(*) AS num_raw_txs,
        SUM(CASE WHEN gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000015' THEN 1 ELSE 0 END) AS l2_num_attr_deposit_txs_per_day,
        SUM(CASE WHEN gas_price = 0 AND to_address = '0x4200000000000000000000000000000000000007' THEN 1 ELSE 0 END) AS l2_num_user_deposit_txs_per_day,
        SUM(CASE WHEN gas_price > 0 THEN 1 ELSE 0 END) AS l2_num_txs_per_day,
        SUM(CASE WHEN receipt_status = 1 AND gas_price > 0 THEN 1 ELSE 0 END) AS l2_num_success_txs_per_day,
        COUNT(DISTINCT from_address) AS num_users_per_day,
        SUM(receipt_gas_used) AS l2_gas_used,
        SUM(receipt_l1_gas_used) AS l1_gas_used_on_l2,
        SUM(receipt_l1_gas_used * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar)) AS l1_gas_paid,
        SUM(receipt_l1_gas_used * receipt_l1_blob_base_fee_scalar) AS blob_gas_paid,
        -- SUM(LENGTH(unhex(input))-1) AS calldata_bytes_l2_per_day,
        SUM( input_bytes ) AS calldata_bytes_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used * COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar) ELSE 0 END) AS l1_gas_paid_user_txs,
        SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used * receipt_l1_blob_base_fee_scalar ELSE 0 END) AS blob_gas_paid_user_txs,
        
        
        SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used ELSE 0 END) AS l1_gas_used_user_txs_l2_per_day,
        -- SUM(CASE WHEN gas_price > 0 THEN (LENGTH(unhex(input))-1) ELSE 0 END) AS calldata_bytes_user_txs_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN input_bytes ELSE 0 END) AS calldata_bytes_user_txs_l2_per_day,
        SUM(CASE WHEN gas_price > 0 THEN receipt_gas_used ELSE 0 END) AS l2_gas_used_user_txs_per_day,
        SUM((gas_price * receipt_gas_used + receipt_l1_fee) / 1e18) AS l2_eth_fees_per_day,
        SUM(receipt_l1_fee / 1e18) AS l1_contrib_l2_eth_fees_per_day,
        SUM(receipt_l1_gas_used*COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar)*receipt_l1_gas_price / 1e18) AS l1_l1gas_contrib_l2_eth_fees_per_day,
        SUM(receipt_l1_gas_used*receipt_l1_blob_base_fee_scalar*receipt_l1_blob_base_fee / 1e18) AS l1_blobgas_contrib_l2_eth_fees_per_day,
        SUM((gas_price * receipt_gas_used) / 1e18) AS l2_contrib_l2_eth_fees_per_day,
        SUM(((gas_price - max_priority_fee_per_gas) * receipt_gas_used) / 1e18) AS l2_contrib_l2_eth_fees_base_fee_per_day,
        SUM((max_priority_fee_per_gas * receipt_gas_used) / 1e18) AS l2_contrib_l2_eth_fees_priority_fee_per_day,

        -- SUM(16 * (LENGTH(REPLACE(TO_HEX(input), '00', '')) - 1) + 4 * (LENGTH(unhex(input)) - 1 - (LENGTH(REPLACE(TO_HEX(input), '00', '')) - 1))) AS input_calldata_gas_l2_per_day,
          -- SUM(
          -- 4 * ((LENGTH(SUBSTR(input, 3)) - LENGTH(REGEXP_REPLACE(SUBSTR(input, 3), '(00)+', ''))) / 2) --zero bytes
          -- + 16 * ((LENGTH(REGEXP_REPLACE(SUBSTR(input, 3), '(00)+', ''))) / 2) --nonzero bytes
          -- ) AS input_calldata_gas_l2_per_day,
        -- SUM(CASE WHEN gas_price > 0 THEN 16 * (LENGTH(REPLACE(TO_HEX(input), '00', '')) - 1) + 4 * (LENGTH(unhex(input)) - 1 - (LENGTH(REPLACE(TO_HEX(input), '00', '')) - 1)) ELSE 0 END) AS input_calldata_gas_user_txs_l2_per_day,
          -- SUM( case when gas_price > 0 then
          -- 4 * ((LENGTH(SUBSTR(input, 3)) - LENGTH(REGEXP_REPLACE(SUBSTR(input, 3), '(00)+', ''))) / 2) --zero bytes
          -- + 16 * ((LENGTH(REGEXP_REPLACE(SUBSTR(input, 3), '(00)+', ''))) / 2) --nonzero bytes
          -- else 0 end ) AS input_calldata_gas_l2_per_day,

        coalesce( SUM(receipt_l1_gas_used / 16) ,0) AS compressedtxsize_approx_l2_per_day,
        coalesce( SUM(CASE WHEN gas_price > 0 THEN receipt_l1_gas_used / 16 ELSE 0 END) ,0) AS compressedtxsize_approx_user_txs_l2_per_day,
        coalesce( COUNT(DISTINCT block_number) ,0) AS num_blocks,
        coalesce( AVG(CASE WHEN gas_price > 0 THEN COALESCE(receipt_l1_base_fee_scalar,receipt_l1_fee_scalar) ELSE NULL END) ,0) AS avg_l1_fee_scalar,
        coalesce( AVG(CASE WHEN gas_price > 0 THEN receipt_l1_blob_base_fee_scalar ELSE NULL END) ,0) AS avg_l1_blob_fee_scalar,
        coalesce( SUM(receipt_l1_gas_price * receipt_l1_gas_used) / SUM(receipt_l1_gas_used) / 1e9 ,0) AS avg_l1_gas_price_on_l2,
        coalesce( SUM(CASE WHEN gas_price > 0 THEN receipt_gas_used * gas_price / 1e9 ELSE NULL END) / SUM(CASE WHEN gas_price > 0 THEN receipt_gas_used ELSE NULL END) ,0) AS avg_l2_gas_price,
        coalesce( SUM(CASE WHEN gas_price > 0 THEN receipt_gas_used * (gas_price - max_priority_fee_per_gas) / 1e9 ELSE NULL END) / SUM(CASE WHEN gas_price > 0 THEN receipt_gas_used ELSE NULL END) ,0) AS base_fee_gwei,
        coalesce( SUM(CASE WHEN gas_price = 0 THEN 0 ELSE (receipt_gas_used * receipt_l1_gas_price) / 1e18 END) ,0) AS equivalent_l1_tx_fee
    FROM
        (
            SELECT 'frax' as chain, 'mainnet' as network, block_timestamp, block_number, `hash`, to_address, from_address, gas_price, max_priority_fee_per_gas, receipt_status
              , receipt_gas_used, receipt_l1_gas_price, receipt_l1_gas_used, receipt_l1_fee, LENGTH(SUBSTR(input, 3)) / 2 AS input_bytes
              , receipt_l1_fee_scalar, receipt_l1_blob_base_fee, receipt_l1_blob_base_fee_scalar, receipt_l1_base_fee_scalar
              , 'frxETH' as gas_token_symbol
              FROM `opensource-observer.superchain.frax_transactions` 
              -- WHERE block_timestamp >= CURRENT_TIMESTAMP() - interval '365' day
        ) txs
    GROUP BY
        1, 2, 3, 4
)

SELECT *
    , num_blocks * 2 as active_secs_per_day
    
    , l2_num_txs_per_day / num_blocks AS l2_num_txs_per_day_per_block
        , l2_num_txs_per_day / (num_blocks*2) AS num_user_txs_per_second
        
    , l2_gas_used / num_blocks AS l2_gas_used_per_block
        , l2_gas_used / (num_blocks*2) AS l2_gas_used_per_second
        
    , l2_eth_fees_per_day / num_blocks AS l2_eth_fees_per_block
        , l2_eth_fees_per_day / (num_blocks*2) AS l2_eth_fees_per_second
    
    , CASE WHEN dt = MIN(dt) OVER (PARTITION BY chain, network) THEN 1 ELSE 0 END AS is_initial_day
    
    , chain as chain_name --make name align with registry

FROM chain_data