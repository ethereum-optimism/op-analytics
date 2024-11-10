WITH

projected_blocks AS (
  SELECT
    number,
    base_fee_per_gas
  FROM
    blocks
),

-- Select the columns that we want from transactions joined to blocks.
-- Include some minor transformations that are needed for further calculations.
projected_transactions1 AS (
  SELECT
    -- Transaction fields
    t.dt,
    t.chain,
    t.chain_id,
    t.nonce,
    t.from_address,
    t.to_address,
    t.block_number,
    t.block_timestamp,
    t.gas_price,
    t.receipt_gas_used,
    t.receipt_l1_gas_used,
    -- Fees
    t.receipt_l1_fee AS l1_fee,
    t.receipt_l1_gas_price,
    t.receipt_l1_blob_base_fee,
    -- L2 Fees  and breakdown into BASE and PRIORITY contributions.
    t.gas_price * t.receipt_gas_used AS l2_fee,
    t.max_priority_fee_per_gas * t.receipt_gas_used AS l2_priority_fee,
    b.base_fee_per_gas * t.receipt_gas_used AS l2_base_fee,
    -- Convenience columns
    epoch_to_hour(t.block_timestamp) AS block_hour,
    substring(t.input, 1, 10) AS method_id,
    t.receipt_status = 1 AS success,
    -- Fee scalars
    coalesce(16 * micro(t.receipt_l1_base_fee_scalar), t.receipt_l1_fee_scalar)
      AS l1_base_scalar,
    coalesce(micro(t.receipt_l1_blob_base_fee_scalar), 0) AS l1_blob_scalar
  FROM transactions AS t
  INNER JOIN projected_blocks AS b ON t.block_number = b.number
  WHERE
    t.gas_price > 0
    -- Optional address filter for faster results when developing.
    -- AND from_address LIKE '0x00%'  
),


-- Add scaled prices. 
-- This reuses the computed l1 scalars.
projected_transactions2 AS (
  SELECT
    *,
    l1_fee / (
      (l1_base_scalar * receipt_l1_gas_price)
      + (l1_blob_scalar * receipt_l1_blob_base_fee)
    ) AS l1_estimated_size
  FROM projected_transactions1
),

-- Add more fee calculations.
-- Reuses results from the previous CTEs.
projected_transactions3 AS (
  SELECT
    *,
    --
    -- Total fee
    l2_fee + l1_fee AS total_fee,
    -- 
    -- L2 Legacy Fee
    if(l2_priority_fee = 0, l2_fee + l1_fee - l2_base_fee, 0) AS l2_base_legacy,
    --
    -- L1 Gas Used
    coalesce(receipt_l1_gas_used, 16 * l1_estimated_size) AS l1_gas_used,
    -- 
    -- L1 Base 
    l1_estimated_size * l1_base_scalar AS l1_base_scaled_size,
    l1_estimated_size * l1_base_scalar * receipt_l1_gas_price AS l1_base_fee,
    -- 
    -- L1 Blob
    l1_estimated_size * l1_blob_scalar AS l1_blob_scaled_size,
    l1_estimated_size * l1_blob_scalar * receipt_l1_blob_base_fee AS l1_blob_fee
  FROM projected_transactions2
)

SELECT
  dt,
  chain,
  chain_id,
  from_address AS address,
  -- Aggregates

  count(*) AS total_txs,

  count(if(success, 1, NULL)) AS total_txs_success,

  count(DISTINCT block_number) AS total_blocks,

  count(DISTINCT if(success, block_number, NULL)) AS total_blocks_success,

  min(block_number) AS min_block_number,

  max(block_number) AS max_block_number,

  max(block_number) - min(block_number) + 1 AS block_interval_active,

  min(nonce) AS min_nonce,

  max(nonce) AS max_nonce,

  max(nonce) - min(nonce) + 1 AS nonce_interval_active,

  min(block_timestamp) AS min_block_timestamp,

  max(block_timestamp) AS max_block_timestamp,

  max(block_timestamp) - min(block_timestamp) AS time_interval_active,

  count(DISTINCT block_hour) AS unique_hours_active,

  count(DISTINCT to_address) AS num_to_addresses,

  count(DISTINCT if(success, to_address, NULL)) AS num_to_addresses_success,

  count(DISTINCT method_id) AS num_method_ids,

  sum(receipt_gas_used) AS total_l2_gas_used,

  sum(if(success, receipt_gas_used, 0)) AS total_l2_gas_used_success,

  sum(l1_gas_used) AS total_l1_gas_used,

  sum(if(success, l1_gas_used, 0)) AS total_l1_gas_used_success,

  wei_to_eth(sum(total_fee)) AS total_gas_fees,

  wei_to_eth(sum(if(success, total_fee, 0))) AS total_gas_fees_success,

  wei_to_eth(sum(l2_fee)) AS l2_contrib_gas_fees,

  wei_to_eth(sum(l1_fee)) AS l1_contrib_gas_fees,

  wei_to_eth(sum(l1_blob_fee)) AS l1_contrib_contrib_gas_fees_blobgas,

  wei_to_eth(sum(l1_base_fee)) AS l1_contrib_gas_fees_l1gas,

  wei_to_eth(sum(l2_base_fee)) AS l2_contrib_gas_fees_basefee,

  wei_to_eth(sum(l2_priority_fee)) AS l2_contrib_gas_fees_priorityfee,

  wei_to_eth(sum(l2_base_legacy)) AS l2_contrib_gas_fees_legacyfee,

  wei_to_gwei(safe_div(sum(l2_fee), sum(receipt_gas_used)))
    AS avg_l2_gas_price_gwei,

  wei_to_gwei(safe_div(sum(l2_base_fee), sum(receipt_gas_used)))
    AS avg_l2_base_fee_gwei,

  wei_to_gwei(safe_div(sum(l2_priority_fee), sum(receipt_gas_used)))
    AS avg_l2_priority_fee_gwei,

  wei_to_gwei(safe_div(sum(l1_base_fee), sum(l1_base_scaled_size))
  ) AS avg_l1_gas_price_gwei,

  wei_to_gwei(safe_div(sum(l1_blob_fee), sum(l1_blob_scaled_size)))
    AS avg_l1_blob_base_fee_gwei
FROM
  projected_transactions3
GROUP BY
  1,
  2,
  3,
  4
