/**

Update first seen erc20 transfers.

*/

SELECT
  chain
  , chain_id
  , contract_address

  -- transfers seen earlier have greater ReplacingMergeTree row_version
  , min(block_timestamp) AS first_seen
  , dateDiff('second', first_seen, '2106-01-01 00:00:00'::DATETIME) AS row_version

FROM INPUT_CLICKHOUSE('blockbatch/token_transfers/erc20_transfers_v1')
GROUP BY 1, 2, 3
