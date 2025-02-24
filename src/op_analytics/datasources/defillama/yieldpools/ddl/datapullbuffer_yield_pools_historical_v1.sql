CREATE TABLE datapullbuffer.defillama_yield_pools_historical_v1
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
  , pool_meta String
  , tvl_usd Float64
  , apy Float64
  , apy_base Float64
  , apy_reward Float64

)
ENGINE = ReplacingMergeTree
PARTITION BY process_dt
ORDER BY (process_dt, dt, pool, protocol_slug, chain, symbol)
TTL process_dt + INTERVAL 1 WEEK DELETE;
