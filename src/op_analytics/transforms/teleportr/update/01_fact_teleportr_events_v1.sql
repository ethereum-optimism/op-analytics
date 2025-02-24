INSERT INTO _placeholder_

WITH across_bridge_metadata AS (
    SELECT *
    FROM dailydata_gcs.read_default(
      rootpath = 'chainsmeta/raw_accross_bridge_gsheet_v1'
    )
)
, chain_metadata AS (
  SELECT *
  FROM dailydata_gcs.read_default(
    rootpath = 'chainsmeta/raw_gsheet_v1'
  )
  WHERE mainnet_chain_id is not null
)
, raw_logs AS (
  SELECT
    block_timestamp
    ,dt
    ,block_number
    ,chain
    ,chain_id
    ,address
    ,transaction_hash
    ,indexed_args
    ,data
    ,log_index
  FROM blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = '*'
    , dt = { dtparam: Date }
  )
  WHERE true
  AND topic0 in ('0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f', '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3')
  AND data is not null
  AND data != ''
  AND length(indexed_args) >= 3
)
, raw_transactions AS (
  SELECT
    hash
    ,block_number
    ,chain
    ,chain_id
    ,from_address
    ,to_address
    ,gas_price
    ,receipt_gas_used
    ,receipt_l1_fee
    ,input
    ,SUBSTRING(input, -10) AS integrator_identifier
  FROM blockbatch_gcs.read_date(
    rootpath = 'ingestion/transactions_v1'
    , chain = '*'
    , dt = { dtparam: Date }
  )
  WHERE
    gas_price > 0
    AND (SUBSTRING(input, -10) = '1dc0de0001'
      OR SUBSTRING(input, -10) = '1dc0de0002')
)
, raw_events AS (
  SELECT
    l.block_timestamp AS block_timestamp
    , l.dt AS dt
    , l.block_number AS block_number
    , l.chain AS src_chain
    , l.chain_id AS src_chain_id
    , l.address AS contract_address
    , l.transaction_hash AS transaction_hash
    , reinterpretAsUInt64(reverse(unhex(substring(indexed_args[2], 3)))) AS deposit_id
    , '0x' || substring(substring(l.data, 3), 25, 40) AS input_token_address
    , '0x' || substring(substring(l.data, 3), 89, 40) AS output_token_address
    , reinterpretAsUInt64(reverse(unhex(substring(indexed_args[1], 3)))) AS dst_chain_id
    , reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 129, 64)))) AS input_amount
    , reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 193, 64)))) AS output_amount
    , reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 257, 64)))) AS quote_timestamp
    , reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 321, 64)))) AS fill_deadline
    , reinterpretAsUInt256(reverse(unhex(substring(substring(l.data, 3), 385, 64)))) AS exclusivity_deadline
    , '0x' || right(substring(substring(l.data, 3), 449, 64), 40) AS recipient_address
    , '0x' || right(substring(substring(l.data, 3), 513, 64), 40) AS relayer_address
    , t.from_address AS depositor_address
    , CASE
      WHEN t.integrator_identifier = '1dc0de0001' THEN 'SuperBridge'
      WHEN t.integrator_identifier = '1dc0de0002' THEN 'Brid.gg'
      ELSE null
    END AS integrator
    , l.log_index AS log_index
    , (t.gas_price * t.receipt_gas_used) / 1e18 AS l2_fee_eth
    , (t.receipt_l1_fee) / 1e18 AS l1_fee_eth
    , (t.gas_price * t.receipt_gas_used) / 1e18 + (t.receipt_l1_fee) / 1e18 AS total_fee_eth
  FROM raw_logs AS l
  JOIN raw_transactions AS t
    ON
      l.transaction_hash = t.hash
      AND l.block_number = t.block_number
      AND l.chain = t.chain
  JOIN
    across_bridge_metadata AS c
    ON
      l.chain = c.chain_name
      AND l.address = lower(c.spokepool_address)
-- AND l.block_timestamp > '2024-05-01'
)


SELECT
  x.block_timestamp
  , x.dt
  , x.block_number
  , x.src_chain
  , x.src_chain_id
  , CASE
    WHEN x.dst_chain_id = 1 THEN 'ethereum'
    ELSE c.chain_name
  END AS dst_chain
  , x.dst_chain_id
  , x.contract_address
  , x.transaction_hash
  , x.deposit_id
  , x.input_token_address
  , mi.symbol AS input_token_symbol
  , x.input_amount AS input_amount_raw
  , x.input_amount / exp10(mi.decimals) AS input_amount
  , x.output_token_address
  , case
      when (x.output_token_address = '0x0000000000000000000000000000000000000000' or mo.chain_id is null or x.dst_chain_id = 1) then mi.symbol
      else mo.symbol
    end as output_token_symbol
  , x.output_amount AS output_amount_raw
  , case
      when (x.output_token_address = '0x0000000000000000000000000000000000000000' or mo.chain_id is null or x.dst_chain_id = 1) then x.output_amount / exp10(mi.decimals)
      else coalesce(x.output_amount / exp10(mo.decimals), x.output_amount / exp10(mi.decimals))
    end as output_amount
  , x.quote_timestamp
  , x.fill_deadline
  , x.exclusivity_deadline
  , x.recipient_address
  , x.relayer_address
  , x.depositor_address
  , x.integrator
  , x.l2_fee_eth
  , x.l1_fee_eth
  , x.total_fee_eth
  , x.log_index
FROM raw_events AS x
LEFT JOIN
  chain_metadata AS c
  ON cast(x.dst_chain_id as String) = cast(c.mainnet_chain_id as String)
LEFT JOIN chainsmeta.dim_erc20_token_metadata_v1 AS mi
  ON
    x.input_token_address = mi.contract_address
    AND cast(x.src_chain_id as String) = cast(mi.chain_id as String)
LEFT JOIN chainsmeta.dim_erc20_token_metadata_v1 AS mo
  ON
    x.output_token_address = mo.contract_address
    AND cast(x.dst_chain_id as String) = cast(mo.chain_id as String)
SETTINGS join_use_nulls = 1, use_hive_partitioning = 1
