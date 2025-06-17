-- This fact table stores daily price data from CoinGecko for tokens.
-- The table is partitioned by date and clustered by token_id for efficient querying.

CREATE TABLE IF NOT EXISTS `op-analytics.coingecko.fact_coingecko_daily_prices_v1`
(
    `token_id` STRING,
    `date` DATE,
    `price_usd` FLOAT64,
    `market_cap_usd` FLOAT64,
    `total_volume_usd` FLOAT64,
    `last_updated` TIMESTAMP,
)
PARTITION BY date
CLUSTER BY token_id
OPTIONS(
    description="Daily token price data from CoinGecko API"
); 