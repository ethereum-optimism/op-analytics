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
    l2_fee + l1_fee AS tx_fee,
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

  count(*) AS tx_cnt,

  count(if(success, 1, NULL)) AS success_tx_cnt,

  count(DISTINCT block_number) AS block_cnt,

  count(DISTINCT if(success, block_number, NULL)) AS success_block_cnt,

  min(block_number) AS block_number_min,

  max(block_number) AS block_number_max,

  max(block_number) - min(block_number) + 1 AS active_block_range,

  min(nonce) AS nonce_min,

  max(nonce) AS nonce_max,

  max(nonce) - min(nonce) + 1 AS active_nonce_range,

  min(block_timestamp) AS block_timestamp_min,

  max(block_timestamp) AS block_timestamp_max,

  max(block_timestamp) - min(block_timestamp) AS active_time_range,

  count(DISTINCT block_hour) AS active_hours_ucnt,

  count(DISTINCT to_address) AS to_address_ucnt,

  count(DISTINCT if(success, to_address, NULL)) AS success_to_address_ucnt,

  count(DISTINCT method_id) AS method_id_ucnt,

  sum(receipt_gas_used) AS l2_gas_used_sum,

  sum(if(success, receipt_gas_used, 0)) AS success_l2_gas_used_sum,

  sum(l1_gas_used) AS l1_gas_used_sum,

  sum(if(success, l1_gas_used, 0)) AS success_l1_gas_used_sum,

  wei_to_eth(sum(tx_fee)) AS tx_fee_sum_eth,

  wei_to_eth(sum(if(success, tx_fee, 0))) AS success_tx_fee_sum_eth,

  -- L2 Fee and breakdown into BASE + PRIORITY
  wei_to_eth(sum(l2_fee)) AS l2_fee_sum_eth,

  wei_to_eth(sum(l2_base_fee)) AS l2_base_fee_sum_eth,

  wei_to_eth(sum(l2_priority_fee)) AS l2_priority_fee_sum_eth,

  wei_to_eth(sum(l2_base_legacy)) AS l2_base_legacy_fee_sum_eth,

  -- L1 Fee and breakdown into BASE + BLOB
  wei_to_eth(sum(l1_fee)) AS l1_fee_sum_eth,

  wei_to_eth(sum(l1_base_fee)) AS l1_base_fee_sum_eth,

  wei_to_eth(sum(l1_blob_fee)) AS l1_blob_fee_sum_eth,

  -- L2 Price and breakdonw into BASE + PRIORITY
  wei_to_gwei(safe_div(sum(l2_fee), sum(receipt_gas_used)))
    AS l2_gas_price_avg_gwei,

  wei_to_gwei(safe_div(sum(l2_base_fee), sum(receipt_gas_used)))
    AS l2_base_price_avg_gwei,

  wei_to_gwei(safe_div(sum(l2_priority_fee), sum(receipt_gas_used)))
    AS l2_priority_price_avg_gwei,

  -- L1 Price breakdown into BASE + BLOB
  wei_to_gwei(safe_div(sum(l1_base_fee), sum(l1_base_scaled_size))
  ) AS l1_base_price_avg_gwei,

  wei_to_gwei(safe_div(sum(l1_blob_fee), sum(l1_blob_scaled_size)))
    AS l1_blob_fee_avg_gwei
FROM
  projected_transactions3
GROUP BY
  1,
  2,
  3,
  4
