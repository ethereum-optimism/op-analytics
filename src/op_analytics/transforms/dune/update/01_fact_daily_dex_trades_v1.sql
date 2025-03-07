INSERT INTO _placeholder_

WITH dex_trades AS (
    SELECT *
    FROM
    dailydata_gcs.read_date(
        rootpath = 'dune/daily_dex_trades_summary_v1'
        , dt = { dtparam: Date }
  )
)

, chain_metadata AS (
  SELECT *
  FROM
    dailydata_gcs.read_default(
      rootpath = 'chainsmeta/raw_gsheet_v1'
    )
)

SELECT
  m.mainnet_chain_id AS chain_id
  , coalesce(m.chain_name, t.blockchain) AS chain
  , t.project
  , t.version
  , t.token_pair_address_id
  , t.token_bought_address
  , t.token_sold_address
  , t.token_bought_symbol
  , t.token_sold_symbol
  , t.token_pair
  , t.sum_token_bought_amount
  , t.sum_token_bought_amount_raw
  , t.avg_swap_token_bought_amount
  , t.avg_swap_token_bought_amount_raw
  , t.med_swap_sum_token_bought_amount
  , t.med_swap_token_bought_amount_raw
  , t.sum_token_sold_amount
  , t.sum_token_sold_amount_raw
  , t.avg_swap_token_sold_amount
  , t.avg_swap_token_sold_amount_raw
  , t.med_swap_token_sold_amount
  , t.med_swap_token_sold_amount_raw
  , t.sum_amount_usd
  , t.avg_swap_amount_usd
  , t.med_swap_amount_usd
  , t.count_swaps
  , t.count_transactions
  , t.dt
FROM dex_trades AS t
LEFT JOIN chain_metadata AS m
  ON t.blockchain = m.dune_schema

SETTINGS join_use_nulls = 1, use_hive_partitioning = 1
