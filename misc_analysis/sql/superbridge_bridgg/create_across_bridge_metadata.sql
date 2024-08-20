CREATE TABLE default.across_bridge_metadata (
    chain_name String,
    display_name String,
    mainnet_chain_id String,
    spokepool_address String
)
ENGINE = MergeTree
ORDER BY chain_name;
