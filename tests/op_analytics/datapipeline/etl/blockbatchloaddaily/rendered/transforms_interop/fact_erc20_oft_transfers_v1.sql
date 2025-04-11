INSERT INTO transforms_interop.fact_erc20_oft_transfers_v1
/**

ERC-20 Transfer transactions that also emit an OFTSent event.


IMPORTANT: this approach filters for cases when the same token contract emits
the OFTSent and the Transfer event. So itonly covers OFT Tokens and it does not
conver OFTAdapter tokens.

*/

WITH

oft_sent_events AS ( -- noqa: ST03
  SELECT
    chain_id
    , transaction_hash
    , address AS contract_address

  FROM s3(
        'https://storage.googleapis.com/oplabs-tools-data-sink/ingestion/logs_v1/chain=base/dt=2025-01-01/*.parquet',
        '<GCS_HMAC_ACCESS_KEY>',
        '<GCS_HMAC_SECRET>',
        'parquet'
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
)

SELECT
  t.dt
  , t.chain
  , t.chain_id
  , t.network
  , t.block_timestamp
  , t.block_number
  , t.transaction_hash
  , t.transaction_index
  , t.log_index
  , t.contract_address
  , t.amount
  , t.from_address
  , t.to_address

FROM 
            (
            SELECT
                * 
            FROM blockbatch.token_transfers__erc20_transfers_v1
            WHERE dt = '2025-01-01' AND chain = 'base'
            )
             AS t
WHERE
  (t.chain_id, t.transaction_hash, t.contract_address) IN (oft_sent_events)
