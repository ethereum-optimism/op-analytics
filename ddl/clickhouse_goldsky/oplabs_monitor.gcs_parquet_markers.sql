CREATE DATABASE IF NOT EXISTS etl_monitor;


-- A table to store completion markers for parquet data in GCS.
-- A completion marker indicates that 1 or more parquet files
-- corresponding to a single unit of processing were successfully
-- written out to GCS.
CREATE TABLE IF NOT EXISTS etl_monitor.gcs_parquet_markers
(
    updated_at DateTime DEFAULT now(),
    marker_path String,
    -- 'dt' is the lowest value of 'dt' encountered across parquet files saved under this marker
    dt Date,
    total_rows UInt64,
    -- The following 3 arrays always have the same number of elements.
    outputs Nested
    (
        full_path String,
        partition_cols Map(String, String),
        row_count UInt64
    ),
    
    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (marker_path)

