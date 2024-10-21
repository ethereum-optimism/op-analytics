CREATE DATABASE IF NOT EXISTS etl_monitor;


-- A table to store completion markers for parquet data in GCS.
-- A completion marker indicates that 1 or more parquet files
-- corresponding to a single unit of processing were successfully
-- written out to GCS.
CREATE TABLE IF NOT EXISTS etl_monitor.raw_onchain_ingestion_markers
(
    updated_at DateTime DEFAULT now(),
    marker_path String,

    -- 'total_rows' is sum(rows) across parquet files saved under this marker.
    total_rows UInt64,

    -- Details for each parquet output saved under this marker.
    outputs Nested
    (
        full_path String,
        partition_cols Map(String, String),
        row_count UInt64
    ),

    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name String,

    -- Name of the chain.
    chain String,

    -- 'dt' is the min(dt) across parquet files saved under this marker.
    dt Date,
    
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (marker_path)

