/**

Aggregated to keep only the first seen value for each token contract address.

*/

SELECT
  chain
  , chain_id
  , contract_address

  -- events seen earlier have greater ReplacingMergeTree row_version
  , min(block_timestamp) AS first_seen
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version

FROM INPUT_CLICKHOUSE('transforms_interop/fact_erc20_oft_transfers_v1')
GROUP BY 1, 2, 3
