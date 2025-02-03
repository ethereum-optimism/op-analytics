/**

Logs from Layerzero OFTSent events

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
  , 'OFTSent' AS event_name

FROM
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = '*'
    , dt = { dtparam: Date }
  )

WHERE
  -- OFT Docs:
  -- https://docs.layerzero.network/v2/home/token-standards/oft-standard
  -- 
  -- Example Log:
  -- https://optimistic.etherscan.io/tx/0x40ddae2718940c4487af4c02d889510ab47e2e423028b76a3b00ec9bc8c04798#eventlog#21
  -- 
  -- Signature:
  -- OFTSent (
  --    index_topic_1 bytes32 guid, 
  --    uint32 dstEid, 
  --    index_topic_2 address fromAddress, 
  --    uint256 amountSentLD, 
  --    uint256 amountReceivedLD
  -- )
  topic0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'
