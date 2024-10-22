WITH tx_data AS (
 SELECT
    a.*
    ,gas_price/1e9 AS gas_price_gwei
    ,base_fee_per_gas/1e9 AS base_fee_per_gas_gwei
    ,l1_gas_price/1e9 AS l1_gas_price_gwei
    , CASE WHEN is_system_tx = 1 THEN NULL ELSE 
        priority_fee_paid_per_gas/1e9
        END AS priority_fee_paid_per_gas_gwei
 FROM (
    SELECT
    t.block_number, t.block_timestamp, t.hash
    , t.receipt_gas_used AS tx_gas, t.gas_price
    , b.base_fee_per_gas, t.max_fee_per_gas, t.max_priority_fee_per_gas
    , t.gas_price - b.base_fee_per_gas AS priority_fee_paid_per_gas
    , t.receipt_l1_gas_price AS l1_gas_price
    , CASE WHEN transaction_type = 126 OR t.gas_price = 0 THEN 1 ELSE 0 END AS is_system_tx
	FROM public.transactions t
        INNER JOIN public.blocks b
            ON b.number = t.block_number
            AND b.timestamp = t.block_timestamp
            AND b.timestamp > CURRENT_TIMESTAMP - interval '1' day - interval '1' minute
    -- filter to the last day & add a 1 minute buffer since we look at trailing 30 blocks (1 minute)
    WHERE t.block_timestamp > CURRENT_TIMESTAMP - interval '1' day - interval '1' minute
    ) a
 )
   
  , block_data AS (
   SELECT
      block_number, block_timestamp, base_fee_per_gas_gwei, l1_gas_price_gwei
      , COUNT(*) - SUM(is_system_tx) AS num_user_transactions
      , SUM(tx_gas) AS block_gas_used
      , 5000000 - SUM(tx_gas) AS gas_used_target_delta
    , PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY priority_fee_paid_per_gas_gwei) AS priority_fee_paid_per_gas_gwei_1pct
    , PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY priority_fee_paid_per_gas_gwei) AS priority_fee_paid_per_gas_gwei_25pct
    , PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY priority_fee_paid_per_gas_gwei) AS priority_fee_paid_per_gas_gwei_50pct
    , PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY priority_fee_paid_per_gas_gwei) AS priority_fee_paid_per_gas_gwei_75pct
    , PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY priority_fee_paid_per_gas_gwei) AS priority_fee_paid_per_gas_gwei_99pct
    FROM tx_data
    GROUP BY block_number, block_timestamp, base_fee_per_gas_gwei, l1_gas_price_gwei
  )
  
  SELECT *
  FROM (
      SELECT 
      block_number, block_timestamp
      , DENSE_RANK() OVER (ORDER BY block_number ASC) AS asc_block_number_rank
      , DENSE_RANK() OVER (ORDER BY block_number DESC) AS desc_block_number_rank
      , FIRST_VALUE(base_fee_per_gas_gwei) OVER (ORDER BY block_number DESC) AS last_base_fee_per_gas_gwei
      , AVG(base_fee_per_gas_gwei) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_base_fee_per_gas_gwei

      , FIRST_VALUE(l1_gas_price_gwei) OVER (ORDER BY block_number DESC) AS last_l1_gas_price_gwei
      , AVG(l1_gas_price_gwei) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_l1_gas_price_gwei

      , AVG(priority_fee_paid_per_gas_gwei_1pct) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_priority_fee_paid_per_gas_gwei_1pct
      , AVG(priority_fee_paid_per_gas_gwei_25pct) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_priority_fee_paid_per_gas_gwei_25pct
      , AVG(priority_fee_paid_per_gas_gwei_50pct) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_priority_fee_paid_per_gas_gwei_50pct
      , AVG(priority_fee_paid_per_gas_gwei_75pct) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_priority_fee_paid_per_gas_gwei_75pct
      , AVG(priority_fee_paid_per_gas_gwei_99pct) OVER (ORDER BY block_number DESC ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS trailing_avg_priority_fee_paid_per_gas_gwei_99pct

      FROM block_data
      WHERE num_user_transactions > 0
      ) a
  --filter out blocks where we didn't pull full 30 block data
  WHERE asc_block_number_rank >= 30
  
  ORDER BY block_number DESC
