/**

Distinct contract addresses for tokens associated with Wormhone Native Token Transfers (NTT)

Example NTT Delivery() event:
https://basescan.org/tx/0x54dbc52c79ea84d521e46c913eacf903079897a17e17f1eecba9cda206b24118#eventlog#135

*/

INSERT INTO transforms.dim_ntt_contract_addresses_v1



WITH 

ntt_logs AS (
  SELECT
      chain::String AS chain
    , chain_id::Int32 AS chain_id
    , address::FixedString(66) AS contract_address
    , block_number::UInt64 AS block_number
    , transaction_hash::FixedString(66) AS transaction_hash
    , topic0::FixedString(66) as topic0
  FROM
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = '*'
    , dt = {dtparam:Date} -- noqa: LT01,CP02
  )
  WHERE topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  OR topic0 = '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e'
)

, scan1 AS (
SELECT 
  chain
  , chain_id
  , contract_address
  , block_number
  , transaction_hash
FROM
  ntt_logs
WHERE
  topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),

scan2 AS (
SELECT 
  chain_id
  , block_number
  , transaction_hash
FROM
  ntt_logs
WHERE
  topic0 = '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e'
)

SELECT DISTINCT chain, chain_id, scan1.contract_address 
FROM scan1 WHERE  (chain_id, block_number, transaction_hash) IN scan2



SETTINGS use_hive_partitioning = 1 -- noqa: 









WITH

ntt_delivery AS (
  SELECT
    chain
    , chain_id
    , address AS contract_address
    , topic0
    , COUNT(DISTINCT topic0) OVER (PARTITION BY chain, chain_id, block_number, transaction_hash) AS num_distinct_topics
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/logs_v1'
      , chain = '*'
      , dt = {dtparam:Date} -- noqa: LT01,CP02
    )
  WHERE
    --
    --
    -- Delivery(
    --     index_topic_1 address recipientContract,
    --     index_topic_2 uint16 sourceChain,
    --     index_topic_3 uint64 sequence,
    --     bytes32 deliveryVaaHash,
    --     uint8 status,
    --     uint256 gasUsed,
    --     uint8 refundStatus,
    --     bytes additionalStatusInfo,
    --     bytes overridesInfo
    -- )  
    topic0 = '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e'
)

-- Filter to include only Transfer() and NTT Delivery() events. 
-- Count the number of distinct topic0 values observed on each transaction hash.
-- Since we filter for Transfer() or Delivery() topic0 then the COUNT(DISTINCT topic0) is either 1 or 2.
-- The value can only be = 2 when both Transfer() and Delivery() events were seen on the same transaction.
, filtered AS (
  SELECT
    chain
    , chain_id
    , address AS contract_address
    , topic0
    , COUNT(DISTINCT topic0) OVER (PARTITION BY chain, chain_id, block_number, transaction_hash) AS num_distinct_topics
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'ingestion/logs_v1'
      , chain = '*'
      , dt = {dtparam:Date} -- noqa: LT01,CP02
    )
  WHERE
    topic0 IN (
    --
    -- Transfer(
    --     index_topic_1 address from,
    --     index_topic_2 address to,
    --     uint256 value
    -- )
      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
      --
      --
      -- Delivery(
      --     index_topic_1 address recipientContract,
      --     index_topic_2 uint16 sourceChain,
      --     index_topic_3 uint64 sequence,
      --     bytes32 deliveryVaaHash,
      --     uint8 status,
      --     uint256 gasUsed,
      --     uint8 refundStatus,
      --     bytes additionalStatusInfo,
      --     bytes overridesInfo
      -- )
      , '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e'
    )
)

-- Filter to transactions that have both Transfer() and Delivery() events.
-- Keep the contract address that emitted the Transfer() event.
SELECT DISTINCT
  chain
  , chain_id
  , contract_address
FROM filtered
WHERE
  num_distinct_topics = 2
  AND topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
SETTINGS use_hive_partitioning = 1 -- noqa:
