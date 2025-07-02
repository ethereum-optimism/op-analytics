CREATE TABLE IF NOT EXISTS _placeholder_ (
    chain String,
    address String,
    tokens Array(String),
    expected_chains Array(String),
    end_date Nullable(String),
    chain_id Nullable(Int32),
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
    INDEX address_idx address TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY (chain, address) 