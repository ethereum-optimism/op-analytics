INSERT INTO transforms_interop.dim_erc20_first_seen_v1
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

FROM 
            (
            SELECT
                * 
            FROM blockbatch.token_transfers__erc20_transfers_v1
            WHERE dt = '2025-01-01' AND chain = 'base'
            )
            
GROUP BY 1, 2, 3
