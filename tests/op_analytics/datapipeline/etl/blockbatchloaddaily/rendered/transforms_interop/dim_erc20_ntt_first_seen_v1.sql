INSERT INTO transforms_interop.dim_erc20_ntt_first_seen_v1
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

FROM 
            (
            SELECT
                * 
            FROM transforms_interop.fact_erc20_ntt_transfers_v1
            WHERE dt = '2025-01-01' AND chain = 'base'
            )
            
GROUP BY 1, 2, 3
