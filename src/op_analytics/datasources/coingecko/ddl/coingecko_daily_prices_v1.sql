-- This fact table stores daily price data from CoinGecko for tokens.
-- The table is partitioned by date and clustered by token_id for efficient querying.

CREATE TABLE IF NOT EXISTS _placeholder_
(
    `token_id` String,
    `dt` Date,
    `price_usd` Float64,
    `market_cap_usd` Float64,
    `total_volume_usd` Float64,
    `last_updated` String,
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX token_id_idx token_id TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY dt
ORDER BY (dt, token_id)
TTL dt + INTERVAL 1 WEEK DELETE; 