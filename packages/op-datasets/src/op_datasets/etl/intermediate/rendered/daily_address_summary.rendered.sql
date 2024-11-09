WITH projected_blocks AS (
  SELECT
    number,
    base_fee_per_gas
  FROM
    blocks
)

SELECT
  t.dt,
  t.chain,
  t.chain_id,
  t.from_address AS address,
  -- Aggregates
  
  COUNT(hash) AS total_txs,
  
  COUNT(IF(receipt_status = 1, 1, NULL)) AS total_txs_success,
  
  COUNT(DISTINCT block_number) AS total_blocks,
  
  COUNT(DISTINCT IF(receipt_status = 1, block_number, NULL)) AS total_blocks_success,
  
  MIN(block_number) AS min_block_number,
  
  MAX(block_number) AS max_block_number,
  
  MAX(block_number) - MIN(block_number) + 1 AS block_interval_active,
  
  MIN(nonce) AS min_nonce,
  
  MAX(nonce) AS max_nonce,
  
  MAX(nonce) - MIN(nonce) + 1 AS nonce_interval_active,
  
  MIN(block_timestamp) AS min_block_timestamp,
  
  MAX(block_timestamp) AS max_block_timestamp,
  
  MAX(block_timestamp) - MIN(block_timestamp) AS time_interval_active,
  
  COUNT(DISTINCT epoch_to_hour(block_timestamp)) AS unique_hours_active,
  
  COUNT(DISTINCT to_address) AS num_to_addresses,
  
  COUNT(DISTINCT IF(receipt_status = 1, to_address, NULL)) AS num_to_addresses_success,
  
  COUNT(DISTINCT SUBSTRING(input,1,10)) AS num_method_ids,
  
  SUM(receipt_gas_used) AS total_l2_gas_used,
  
  SUM(IF(receipt_status = 1, receipt_gas_used, 0)) AS total_l2_gas_used_success,
  
  SUM(COALESCE(receipt_l1_gas_used, (16 * receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))))) AS total_l1_gas_used,
  
  SUM(IF(receipt_status = 1, COALESCE(receipt_l1_gas_used, (16 * receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0)))), 0)) AS total_l1_gas_used_success,
  
  wei_to_eth(SUM((gas_price * receipt_gas_used) + receipt_l1_fee)) AS total_gas_fees,
  
  wei_to_eth(SUM(IF(receipt_status = 1, (gas_price * receipt_gas_used) + receipt_l1_fee, 0))) AS total_gas_fees_success,
  
  wei_to_eth(SUM((gas_price * receipt_gas_used))) AS l2_contrib_gas_fees,
  
  wei_to_eth(SUM(receipt_l1_fee)) AS l1_contrib_gas_fees,
  
  wei_to_eth(SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) AS l1_contrib_contrib_gas_fees_blobgas,
  
  wei_to_eth(SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price)) AS l1_contrib_gas_fees_l1gas,
  
  wei_to_eth(SUM((base_fee_per_gas * receipt_gas_used))) AS l2_contrib_gas_fees_basefee,
  
  wei_to_eth(SUM((max_priority_fee_per_gas * receipt_gas_used))) AS l2_contrib_gas_fees_priorityfee,
  
  wei_to_eth(SUM(IF(max_priority_fee_per_gas=0, (gas_price * receipt_gas_used) + receipt_l1_fee - (max_priority_fee_per_gas * receipt_gas_used) - (base_fee_per_gas * receipt_gas_used), 0))) AS l2_contrib_gas_fees_legacyfee,
  
  wei_to_gwei(safe_div(SUM((gas_price * receipt_gas_used)), SUM(receipt_gas_used))) AS avg_l2_gas_price_gwei,
  
  wei_to_gwei(safe_div(SUM((base_fee_per_gas * receipt_gas_used)), SUM(receipt_gas_used))) AS avg_l2_base_fee_gwei,
  
  wei_to_gwei(safe_div(SUM((max_priority_fee_per_gas * receipt_gas_used)), SUM(receipt_gas_used))) AS avg_l2_priority_fee_gwei,
  
  wei_to_gwei(safe_div(SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price), SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar)))) AS avg_l1_gas_price_gwei,
  
  wei_to_gwei(safe_div(SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0)), SUM((receipt_l1_fee / (COALESCE(16*micro(receipt_l1_base_fee_scalar), receipt_l1_fee_scalar) * receipt_l1_gas_price + COALESCE(micro(receipt_l1_blob_base_fee_scalar) * receipt_l1_blob_base_fee, 0))) * micro(receipt_l1_blob_base_fee_scalar)))) AS avg_l1_blob_base_fee_gwei,
  FROM
  transactions AS t
INNER JOIN projected_blocks AS b ON t.block_number = b.number
WHERE
  t.gas_price > 0
  -- Optional address filter for faster results when developing.
  -- AND from_address LIKE '0x00%'
GROUP BY
  1,
  2,
  3,
  4