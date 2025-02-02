/**

Logs from Wormhole Native Token Transfer (NTT) transactions.

*/
INSERT INTO transforms_{db}.{table}

SELECT
  , dt::Date AS dt
  , chain::String AS chain
  , chain_id::Int32 AS chain_id
  , block_timestamp::DateTime AS block_timestamp
  , block_number::UInt64 AS block_number
  , transaction_hash::FixedString(66) AS transaction_hash
  , transaction_index UInt64 AS transaction_index
  , log_index UInt64 AS log_index
  , , address::FixedString(66) AS contract_address
  , topic0::FixedString(66) as    topic0
  , CASE WHEN topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'Transfer'
    WHEN topic0 = '0xbccc00b713f54173962e7de6098f643d8ebf53d488d71f4b2a5171496d038f9e' THEN 'Delivery'
    ELSE 'NA'
    END AS event_name


FROM
blockbatch_gcs.read_date(
  rootpath = 'ingestion/logs_v1'
  , chain = '*'
  , dt = {{dtparam:Date}} -- noqa: LT01,CP02
)



  WHERE
    --
    -- Transfer(
    --     index_topic_1 address from,
    --     index_topic_2 address to,
    --     uint256 value
    -- )
    topic0 ='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    OR
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
