CREATE DATABASE IF NOT EXISTS etl_monitor;


-- A table to store completion markers for parquet data in GCS.
-- A completion marker indicates that 1 or more parquet files
-- corresponding to a single unit of processing were successfully
-- written out to GCS.
CREATE TABLE IF NOT EXISTS etl_monitor.raw_onchain_ingestion_markers
(
    updated_at DateTime DEFAULT now(),
    marker_path String,
    dataset_name String,
    root_path String,

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

    -- Number of blocks in this batch
    num_blocks Int32,

    -- Min and max block numbers in this batch
    min_block Int64,
    max_block Int64,

    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (marker_path)

-- The number of parts covered by this marker
ALTER TABLE etl_monitor.raw_onchain_ingestion_markers ADD COLUMN IF NOT EXISTS num_parts UInt32 AFTER root_path