
CREATE TABLE IF NOT EXISTS etl_monitor.daily_data_markers
(
    updated_at TIMESTAMP,
    marker_path STRING,
    dataset_name STRING,
    root_path STRING,

    -- The number of parts covered by this marker
    num_parts UINT32,

    -- Details for each parquet output saved under this marker.
    data_path String,
    row_count UInt64,


    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name STRING,

    -- Hostname of the machine where the ingeston process was running
    writer_name String,


    -- NON-STANDARD FIELDS BELOW

    dt Date,

)
