CREATE DATABASE IF NOT EXISTS etl_monitor;


CREATE TABLE IF NOT EXISTS etl_monitor.intermediate_model_markers
(
    updated_at DateTime DEFAULT now(),
    marker_path String,
    dataset_name String,
    root_path String,
    num_parts UInt32,

    -- Details for each parquet output saved under this marker.
    data_path String,
    row_count UInt64,

    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name String,

    -- Hostname of the machine where the ingeston process was running
    writer_name String,

    -- NON-STANDARD FIELDS BELOW    

    -- Name of the chain.
    chain String,

    dt Date,

    -- Name of the model that produced the dataset
    model_name String,

    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (marker_path)
