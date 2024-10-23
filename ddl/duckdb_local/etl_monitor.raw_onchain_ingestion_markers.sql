-- A table to store completion markers for parquet data in GCS.
-- A completion marker indicates that 1 or more parquet files
-- corresponding to a single unit of processing were successfully
-- written out to GCS.
CREATE TABLE IF NOT EXISTS etl_monitor.raw_onchain_ingestion_markers
(
    updated_at TIMESTAMP,
    marker_path STRING,

    -- 'total_rows' is sum(rows) across parquet files saved under this marker.
    total_rows BIGINT,

    -- Details for each parquet output saved under this marker.
    outputs STRUCT(full_path STRING, partition_cols STRING[2][], row_count BIGINT)[],


    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name STRING,

    -- Name of the chain.
    chain STRING,

    -- 'dt' is the min(dt) across parquet files saved under this marker.
    dt DATE
)

