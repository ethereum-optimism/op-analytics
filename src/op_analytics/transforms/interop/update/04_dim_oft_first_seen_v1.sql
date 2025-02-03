/**

Update with OFTSent events, keeping ttrack of first seen.

*/

INSERT INTO _placeholder_

SELECT
  t.chain
  , t.chain_id
  , t.contract_address

  -- transfers seen earlier have greater row_version
  , min(t.block_timestamp) AS first_seen
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version

FROM transforms_interop.fact_oft_sent_events_v1 AS t
GROUP BY 1, 2, 3
