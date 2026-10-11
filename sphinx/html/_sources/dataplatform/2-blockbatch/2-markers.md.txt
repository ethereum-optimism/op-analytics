# Marker Metadata Layer

Observability is perhaps the most important aspect of any data pipeline. How can we be sure it is
working as expected if we can't see what is happening? 

For `blockbatch` processing  (and also for several other patterns at OP Labs) we introduce the idea
of data pipeline `markers`. This is simple: there is one marker for each parquet file written to 
the data lake, and we have separate markers for each of the different `blockbatch` data processing
steps.

## Why are markers needed?

Since locations in the data lake are deterministic (modulo the `dt` value), we can use the data lake
itself as the source of truth for data that has been processed or needs to be processed. 

For example, what blocks have been ingested for chain=op in the last 5 days? We could answer this
question by listing the contents of the GCS bucket. However, the listing approach is very inefficient
so instead we store metadata for each parquet file as a marker, which is written as a row in the
`blockbatch_markers` table in ClickHouse cloud.  At OP Labs we rely heavily on ClickHouse cloud, 
so it was a natural choice to store the markers there. 

In summary, the reason markers exist is so we can very quickly and cheaply answer questions about
what data do we have in the data lake. 

## Monitoring

The `blockbatch_markers` table in ClickHouse stores the following metadata for each parquet file in
the data lake:

- `chain`: The chain the data belongs to.
- `dt`: The date partition.
- `root_path`: The logical name of the data artifact.
- `data_path`: The physical path of the parquet file in the data lake.
- `min_block`: The minimum block number in the parquet file.
- `max_block`: The maximum block number in the parquet file.
- `row_count`: The number of rows in the parquet file.
- `num_parts`: The number of date partitions associated with the batch that the file belongs to.
- `process_name`: The name of the process that wrote the parquet file.
- `writer_name`: The name of the machine where the process was running.
- `updated_at`: The timestamp of when the process wrote the parquet file.

For a given chain we know in advance how many blocks are produced per day (usually 1/s or 2/s). 
So for a given date we can come up with an expectation of the number of blocks that should be
processed on a given `dt`.  The markers table let's us very quickly query how many blocks were
actually processed for a given `dt`, and this is in fact what we use for our pipeline monitoring
Hex dashboards (which can be found at `go/pipeline`).

After data is ingested we can use the ingestion markers to formulate the expectation for data that
needs to be processed across the different `blockbatch` models that we support.  That expectation
and markers for the output model `root_paths` are used to monitor the data processing parts of
the pipeline.

## Job Planning

Job planning is no different than processing. We can find out what data has been ingested and not
processed yet and then schedule work accordingly. In that way the `blockbatch_markers` table acts
like a processing queue for pending work.



