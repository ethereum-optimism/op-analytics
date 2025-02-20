WITH raw_events AS (
  SELECT
    l.block_timestamp AS block_timestamp
    , l.block_number AS block_number
    , l.chain AS src_chain
    , l.chain_id AS src_chain_id
    , l.address AS contract_address
    , l.transaction_hash AS transaction_hash
    , REINTERPRETASUINT64(REVERSE(UNHEX(SUBSTRING(SPLITBYCHAR(',', l.topics)[3], 3)))) AS deposit_id
    , '0x' || SUBSTRING(SUBSTRING(l.data, 3), 25, 40) AS input_token_address
    , '0x' || SUBSTRING(SUBSTRING(l.data, 3), 89, 40) AS output_token_address
    , CAST(REINTERPRETASUINT64(REVERSE(UNHEX(SUBSTRING(SPLITBYCHAR(',', l.topics)[2], 3)))) AS String) AS dst_chain_id
    , REINTERPRETASUINT256(REVERSE(UNHEX(SUBSTRING(SUBSTRING(l.data, 3), 129, 64)))) AS input_amount
    , REINTERPRETASUINT256(REVERSE(UNHEX(SUBSTRING(SUBSTRING(l.data, 3), 193, 64)))) AS output_amount
    , REINTERPRETASUINT256(REVERSE(UNHEX(SUBSTRING(SUBSTRING(l.data, 3), 257, 64)))) AS quote_timestamp
    , REINTERPRETASUINT256(REVERSE(UNHEX(SUBSTRING(SUBSTRING(l.data, 3), 321, 64)))) AS fill_deadline
    , REINTERPRETASUINT256(REVERSE(UNHEX(SUBSTRING(SUBSTRING(l.data, 3), 385, 64)))) AS exclusivity_deadline
    , '0x' || RIGHT(SUBSTRING(SUBSTRING(l.data, 3), 449, 64), 40) AS recipient_address
    , '0x' || RIGHT(SUBSTRING(SUBSTRING(l.data, 3), 513, 64), 40) AS relayer_address
    , t.from_address AS depositor_address
    , CASE
      WHEN SUBSTRING(t.input, -10) = '1dc0de0001' THEN 'SuperBridge'
      WHEN SUBSTRING(t.input, -10) = '1dc0de0002' THEN 'Brid.gg'
      ELSE null
    END AS integrator
    , l.log_index AS log_index
    , (t.gas_price * t.gas_used) / 1e18 AS l2_fee_eth
    , (t.receipt_l1_fee) / 1e18 AS l1_fee_eth
    , (t.gas_price * t.gas_used) / 1e18 + (t.receipt_l1_fee) / 1e18 AS total_fee_eth
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/logs_v1'
      , chain = '*'
      , dt = { dtparam: Date }
    ) AS l
  JOIN
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/transactions_v1'
      , chain = '*'
      , dt = { dtparam: Date }
    ) AS t
    ON
      l.transaction_hash = t.hash
      AND l.block_number = t.block_number
      AND l.chain = t.chain
  JOIN
    dailydata_gcs.read_default(
      rootpath = 'chainsmeta/raw_accross_bridge_gsheet_v1'
    ) AS c
    ON
      l.chain = c.chain_name
      AND l.address = c.spokepool_address
  WHERE
    true
    AND SPLITBYCHAR(',', l.topics)[1] = '0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f'
    AND t.gas_price > 0
    AND l.data IS NOT null AND l.data != '' -- info is there
-- AND l.block_timestamp > '2024-05-01'
)

SELECT
  x.block_timestamp
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
  , x.input_amount / 10 ^ mi.decimals AS input_amount
  , x.output_token_address
  , mo.symbol AS output_token_symbol
  , x.output_amount AS output_amount_raw
  , x.output_amount / 10 ^ mo.decimals AS output_amount
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
  dailydata_gcs.read_default(
    rootpath = 'chainsmeta/raw_accross_bridge_gsheet_v1'
  ) AS c
  ON x.dst_chain_id = c.chain_id
LEFT JOIN chainsmeta.dim_erc20_token_metadata_v1 AS mi
  ON
    x.input_token_address = mi.contract_address
    AND x.src_chain_id = mi.chain_id
LEFT JOIN chainsmeta.dim_erc20_token_metadata_v1 AS mo
  ON
    x.output_token_address = mo.contract_address
    AND x.dst_chain_id = mo.chain_id
WHERE integrator IS NOT null
