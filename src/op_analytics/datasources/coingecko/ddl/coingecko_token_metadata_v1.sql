-- CoinGecko token metadata table
-- Stores token information including contract addresses, descriptions, links, etc.

CREATE TABLE IF NOT EXISTS coingecko.coingecko_token_metadata_v1
(
    `dt` String, -- Partition column: date when metadata was fetched
    `token_id` String,
    `name` String,
    `symbol` String,
    `description` String,
    `categories` Array(String),
    `homepage` Array(String),
    `blockchain_site` Array(String),
    `official_forum_url` Array(String),
    `chat_url` Array(String),
    `announcement_url` Array(String),
    `twitter_screen_name` String,
    `telegram_channel_identifier` String,
    `subreddit_url` String,
    `repos_url` String,
    `contract_addresses` String, -- JSON string of platform -> address mapping
    INDEX token_id_idx token_id TYPE minmax GRANULARITY 1,
    INDEX symbol_idx symbol TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(dt)
ORDER BY (token_id)
PARTITION BY dt 