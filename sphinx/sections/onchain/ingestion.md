# Ingestion

## Data Pipeline

```{warning}
Our data pipeline is currently under construction. The information below reflects our latests plans.
```

### Overview Diagram ([link](https://excalidraw.com/#json=1MWxonIgxwWNA5xGOLJuk,W2_v6uGd8zfk_yln9cno6w))

```{image} oplabs-data-pipeline.png
:alt: pipeline
:class: bg-primary
:width: 60%
:align: center
```


### Audited Core Datasets

[Goldsky](https://docs.goldsky.com/) is our current raw onchain data source. Our ingestion process
runs incrementally based on block number. For a given block range it will:
1. Fetch raw onchain data
2. Run audits.
3. If audits pass, then write to audited datsets iceberg tables in GCS.
4. If audits fail, alert the pipeline operators. 

The audit process will help us ensure that the data is valid and self consistent. For example,
there shouldn't be any block nubmer gaps, and the transaction count in a block should match
the number of records in the transactiosn table. 

Audited datsets are stored in GCS as standalone iceberg tables. This makes it easy to access this
data with a variety of query engines, which should future-proof our operations. 

### Processed Datasets

Custom processing also runs incrementally based on block number. Here is where the most upstream
data transformations defined by OP Labs take place. Some are generic, like decoding logs and traces.
Some are more specific, like extracting transfers or user actions from the core data. The data is
also written to GCS as iceberg at this point.

Note that the Ingestion and Processing steps can be fused for efficiency. After auditing we can
run processing with the data in-hand, saving us one write-read trip to GCS. Our plan is to process
data using block batches small enough so that all of the core datsets (transactions, logs, traces)
fit in-memory. Any logic that requires joning across core datasets should be executed during
processing, to take advantage of data locality.  Performing joins downstream after core datsets have
been written out can lead to expensive data shuffling.

The intermediate iceberg tables stored in GCS will be useful when iterating on the processing logic.
We will be able to **backfill** any processed datasets without having to go all the way upstream to
the raw onchain data provider.

### Public Datasets

Public core datasets will be date-partitioned and written to BigQuery. Starting with parquet files
in GCS will allow us to schedule load jobs, which are the most cost effective way to ingest data
into BQ. 

### Downstream Data Modeling

For models downstream of public BigQuery datsets we will use a more standard dbt setup. This
would be similar to other solutions available in the ecosystem, such as [OSO](https://docs.opensource.observer/docs/how-oso-works/architecture#dbt-pipeline) or [Dune Spellbook](https://github.com/duneanalytics/spellbook).
