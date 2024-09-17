CREATE TABLE {view_name}
(
    block_timestamp         DateTime,
    block_number            UInt64,
    transaction_hash        FixedString(66),
    from_address            FixedString(42),
    t.to_address            FixedString(42),
    tx_value                Float64,
    l1_gas_fee              Float64,
    l2_gas_fee              Float64,
    total_gas_fee           Float64,
    chain_name              String,
    insert_time             DateTime
)

ENGINE = ReplacingMergeTree(insert_time)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (block_timestamp, block_number, transaction_hash, chain_name)