CREATE TABLE datapullbuffer.defillama_protocols_tvl_v1
(
  process_dt Date
  , dt Date
  , protocol_slug String
  , chain String
  , total_app_tvl Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY process_dt
ORDER BY (process_dt, dt, protocol_slug, chain)
TTL process_dt + INTERVAL 1 WEEK DELETE;