CREATE DATABASE IF NOT EXISTS oplabs_monitor;

CREATE TABLE IF NOT EXISTS oplabs_monitor.core_datasets
(
    updated_at DateTime DEFAULT now(),
    dt Date,
    chain String,
    block_range_min UInt64,
    block_range_max UInt64,
    output_file_namespace String,
    output_file_name String,
    output_file_path String
)
ENGINE = MergeTree
ORDER BY (dt, chain, block_range_min)
PRIMARY KEY (dt)


