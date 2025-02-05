/**

ERC-20 Transfer transactions that also emit an NTT Delivery event.


IMPORTANT: this approach looks for transactions where the same transaction has
an ERC-20 Transfer followed by an NTT Delivery event. It assumes that contracts that
emit Transfer events prior to a Delivery event in a transaciton are NTT-enabled
token contracts.

Transactions may be simple, with just one Transfer + Delivery, for example:

https://basescan.org/tx/0x3717e2df7d7f070254d5f477a94012f8d17417a2ff6e0f0df7daa767d851808c#eventlog
https://basescan.org/tx/0x39f958600df4faff77320801daea1c9757209f93e653b545d3598596077ad1b8#eventlog

But we can also have mor complex transactions that have several Transfer events
before the Delivery event, for example:

https://optimistic.etherscan.io/tx/0x9ae78927d9771a2bcd89fc9eb467c063753dc30214d4b858e0cb6e02151dc592#eventlog

*/

INSERT INTO _placeholder_

WITH


-- NOTE: The Delivery event is not sent by the token contract, it is sent
-- by the Wormhole Relayer or a proxy.
ntt_delivery_events AS ( -- noqa: ST03
  SELECT
    chain_id
    , transaction_hash
    , log_index

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

FROM blockbatch.token_transfers__erc20_transfers_v1 AS t

INNER JOIN ntt_delivery_events AS n
  ON
    t.chain_id = n.chain_id
    AND t.transaction_hash = n.transaction_hash
WHERE
  t.dt = { dtparam: Date }
  -- Transfer is before Delivery
  AND t.log_index < n.log_index
