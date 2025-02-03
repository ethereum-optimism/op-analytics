INSERT INTO _placeholder_

WITH

erc20_transfers AS (
  SELECT
    chain
    , chain_id
    , contract_address
    , block_number
    , block_timestamp
    , transaction_hash

  FROM
    blockbatch.token_transfers__erc20_transfers_v1
  WHERE dt = { dtparam: Date }
)

, ntt_delivery_events AS ( -- noqa: ST03
  SELECT
    chain_id
    , block_number
    , transaction_hash

  FROM
    transforms_interop.fact_ntt_delivery_events_v1
  WHERE dt = { dtparam: Date }
)

SELECT
  t.chain
  , t.chain_id
  , t.contract_address

  -- transfers seen earlier have greater row_version
  , min(t.block_timestamp) AS first_seen
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version

FROM erc20_transfers AS t
WHERE (t.chain_id, t.block_number, t.transaction_hash) IN (ntt_delivery_events)
GROUP BY 1, 2, 3
