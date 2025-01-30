/**

Daily count of OFTSent Logs per address.

*/

INSERT INTO transforms.dim_oft_contract_addresses_v1

SELECT DISTINCT
  chain
  , chain_id
  , address AS contract_address

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


SETTINGS use_hive_partitioning = 1 -- noqa: 
