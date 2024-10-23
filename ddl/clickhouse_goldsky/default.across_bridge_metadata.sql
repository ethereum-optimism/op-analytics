CREATE TABLE IF NOT EXISTS default.across_bridge_metadata_v2 (
    chain_name String,
    display_name String,
    mainnet_chain_id String,
    spokepool_address String
)
ENGINE = SharedMergeTree
ORDER BY chain_name