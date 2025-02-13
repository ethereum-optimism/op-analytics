CREATE TABLE datapullbuffer.defillama_protocols_token_tvl_v1
(
  process_dt Date
  , dt Date
  , protocol_slug String
  , chain String
  , token String
  , app_token_tvl Float64
  , app_token_tvl_usd Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY process_dt
ORDER BY (process_dt, dt, protocol_slug, chain)
TTL process_dt + INTERVAL 1 WEEK DELETE;
