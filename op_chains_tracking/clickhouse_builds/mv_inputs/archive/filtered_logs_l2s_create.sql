CREATE TABLE {view_name}
(
    transaction_hash    FixedString(66),
    chain_name          String,
    block_timestamp     DateTime,
    block_number        UInt64,
    insert_time         DateTime
)

ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_timestamp, block_number, transaction_hash, chain_name)