CREATE TABLE IF NOT EXISTS revshare_to_addresses (
    address String,
    description String,
    end_date Nullable(String),
    expected_chains Array(String),
    INDEX address_idx address TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY address 