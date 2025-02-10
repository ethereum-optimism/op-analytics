CREATE DATABASE IF NOT EXISTS etl_monitor;


-- Track data ingestion to the data warehouse.
-- 1 row per parquet file
CREATE TABLE IF NOT EXISTS etl_monitor.blockbatch_markers_datawarehouse
(
    updated_at DateTime DEFAULT now(),

    -- Columns that are used to join back to the blockbatch_markers table.
    root_path String,
    chain String,
    dt Date,
    min_block Int64,
    data_path String,

    -- Row count as ingested into the data warehouse.
    loaded_row_count UInt64,

    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name String,

    -- Hostname of the machine where the process was running
    writer_name String,


    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX chain_idx chain TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (root_path, chain, dt, min_block)
