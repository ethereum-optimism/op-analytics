/**

Logs from Wormhole Native Token Transfer (NTT) transactions.

https://wormhole.com/docs/learn/messaging/native-token-transfers/overview/

*/
INSERT INTO _placeholder_

SELECT
  accurateCast(dt, 'Date') AS dt
  , chain
  , chain_id
  , network
  , accurateCast(block_timestamp, 'DateTime') AS block_timestamp
  , accurateCast(block_number, 'UInt64') AS block_number
  , transaction_hash
  , accurateCast(transaction_index, 'UInt64') AS transaction_index
  , accurateCast(log_index, 'UInt64') AS log_index
  , address AS contract_address
  , topic0
  , 'Delivery' AS event_name

FROM
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = '*'
    , dt = { dtparam: Date }
  )

WHERE
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
