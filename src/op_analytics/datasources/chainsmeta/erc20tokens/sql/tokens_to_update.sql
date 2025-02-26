WITH

-- Find the top tokens (by num_tranfers) per chain, looking at the past
-- two days of data.  This is the list of tokens that will be udpated.
top_tokens AS (
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
          t.dt >= subtractDays({ process_dt: Date }, 1)
    ) AS derived
  WHERE
    derived.num_transfers >= derived.percentile_num_transfers
)

-- Tokens that have already been fetched for the "process_dt".
, fetched AS (
  SELECT DISTINCT
    chain
    , chain_id
    , contract_address
  FROM chainsmeta.fact_erc20_token_metadata_v1
  WHERE process_dt = { process_dt: Date }
)

-- Add boolean flag for tokens that have alredy been fetched on the "process_dt".
SELECT
  chain
  , chain_id
  , contract_address
  , (chain, chain_id, contract_address) IN (fetched) AS already_fetched
FROM top_tokens
