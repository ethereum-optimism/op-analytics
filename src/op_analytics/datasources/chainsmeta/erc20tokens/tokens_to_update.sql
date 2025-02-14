-- Find the top contracts (by num_tranfers) per chain on a given day
-- This is the list of contracts for which we are going to fetch fresh
-- token metadata.
SELECT
  derived.chain
  , derived.chain_id
  , derived.contract_address
  , derived.num_transfers
  , derived.percentile_num_transfers
FROM
  (
    SELECT
      t.chain
      , t.chain_id
      , t.contract_address
      , t.num_transfers
      ,quantile({ percentile: Float32 })(t.num_transfers) OVER (PARTITION BY t.chain, t.chain_id) AS percentile_num_transfers
    FROM
        transforms_erc20transfers.fact_erc20_daily_transfers_v1 AS t FINAL 
    WHERE
        t.dt = {dt: Date}
  ) AS derived
WHERE
  derived.num_transfers >= derived.percentile_num_transfers
