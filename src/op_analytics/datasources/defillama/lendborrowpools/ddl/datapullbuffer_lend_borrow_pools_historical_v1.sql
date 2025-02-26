CREATE TABLE datapullbuffer.defillama_lend_borrow_pools_historical_v1
(
  process_dt Date
  , dt Date
  , pool String
  , protocol_slug String
  , chain String
  , symbol String
  , underlying_tokens Array(String)
  , reward_tokens Array(String)
  , il_risk String
  , is_stablecoin Boolean
  , exposure String
  , borrowable Boolean
  , minted_coin String
  , borrow_factor Float64
  , pool_meta String
  --
  , total_supply_usd Int64
  , total_borrow_usd Int64
  , debt_ceiling_usd Int64
  , apy_base Float64
  , apy_reward Float64
  , apy_base_borrow Float64
  , apy_reward_borrow Float64

)
ENGINE = ReplacingMergeTree
PARTITION BY process_dt
ORDER BY (process_dt, dt, pool, protocol_slug, chain, symbol)
TTL process_dt + INTERVAL 1 WEEK DELETE;
