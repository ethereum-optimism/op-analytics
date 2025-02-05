/**

ERC-20 Transfer transactions that also emit an OFTSent event.

Aggregated to keep only the first seen value for each token contract address.

Notice that this approach only covers OFT Tokens and not OFT Adapter tokens.

*/

INSERT INTO _placeholder_

WITH

erc20_transfers AS (
  SELECT
    chain
    , chain_id
    , block_timestamp
    , transaction_hash
    , contract_address

  FROM
    blockbatch.token_transfers__erc20_transfers_v1
  WHERE dt = { dtparam: Date }
)

, oft_sent_events AS ( -- noqa: ST03
  SELECT
    chain_id
    , transaction_hash
    , contract_address

  FROM
    transforms_interop.fact_oft_sent_events_v1
  WHERE dt = { dtparam: Date }
)

SELECT
  t.chain
  , t.chain_id
  , t.contract_address

  -- events seen earlier have greater ReplacingMergeTree row_version
  , min(t.block_timestamp) AS first_seen
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version

FROM erc20_transfers AS t
WHERE (t.chain_id, t.transaction_hash, t.contract_address) IN (oft_sent_events)
GROUP BY 1, 2, 3

