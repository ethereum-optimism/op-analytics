CREATE EXTERNAL TABLE `oplabs-tools-data.gcs_ingestion.superchain_blocks`
WITH PARTITION COLUMNS (chain STRING, dt DATE) 
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://oplabs-tools-data-sink/ingestion/blocks_v1/*'],
    hive_partition_uri_prefix = 'gs://oplabs-tools-data-sink/ingestion/blocks_v1',
    require_hive_partition_filter = true
)