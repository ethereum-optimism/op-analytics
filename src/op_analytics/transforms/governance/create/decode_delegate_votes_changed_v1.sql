CREATE TABLE IF NOT EXISTS _placeholder_
(
    'block_timestamp'   DateTime,
    'block_number'      UInt64,
    'transaction_hash'  String,
    'log_index'         UInt16,
    'raw_topics'        Array(String),
    'delegate'          String,
    'previous_balance'  Float64,
    'new_balance'       Float64,
    'delegation_amount' Float64,
)
ENGINE = ReplacingMergeTree(block_number)
ORDER BY (block_timestamp)