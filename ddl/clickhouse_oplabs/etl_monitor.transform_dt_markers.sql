CREATE DATABASE IF NOT EXISTS etl_monitor;


-- Track execution of dt transforms in the data warehouse.
CREATE TABLE IF NOT EXISTS etl_monitor.transform_dt_markers
(
    updated_at DateTime DEFAULT now(),

    -- Name of the transform.
    transform String,

    -- dt for the marker.
    dt Date,

    -- Any relevant information. JSON string
    metadata String,

    -- Name of the process ingesting data and writing this marker.
    -- Helps us identify who is responsible for writing data. Can
    -- be useful to monitor specific backfills.
    process_name String,

    -- Hostname of the machine where the process was running
    writer_name String,

    INDEX dt_idx dt TYPE minmax GRANULARITY 1,
    INDEX transform_idx transform TYPE minmax GRANULARITY 1,
)
-- Use a merge tree so that we keep around all markers ever written
-- could be useful for auditing purposes.
ENGINE = MergeTree
ORDER BY (transform, dt)
