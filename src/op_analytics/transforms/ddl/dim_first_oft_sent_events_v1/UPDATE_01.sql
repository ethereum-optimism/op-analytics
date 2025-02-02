/**

Distinct contract addresses associated with OFTSent() events

*/

INSERT INTO transforms.dim_first_oft_sent_events_v1


SELECT
  chain
  , chain_id
  , address AS contract_address
  , min(block_timestamp) AS first_seen

  -- We set row_version as the time in seconds to a future timestmap, so that
  -- earlier transfers have a greater row_version. This is needed due to how
  -- the ReplacingMergeTree engine keeps only the max row_version value.
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version  

FROM
  blockbatch_gcs.read_date(
    rootpath = 'ingestion/logs_v1'
    , chain = '*'
    , dt = {dtparam:Date} -- noqa: LT01,CP02
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
GROUP BY 1, 2

SETTINGS use_hive_partitioning = 1 -- noqa: 

