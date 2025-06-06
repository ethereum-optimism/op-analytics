# Onchain Data Pipelines: Blockbatch Load

The data produced by `blockbatch` processing is stored in GCS, so it is not easily accessible for
dashboards and SQL-based data pipelines. In this section we go over how to explore and prototype
with `blockbatch` data in GCS, and how to create jobs that load the data into ClickHouse to power
downstream analytics and dashboards.


## Ad-hoc Blockbatch Queries

We have set up a parameterized view on our ClickHouse instance that allows you to query data from
a blockbatch root path for a given chain and date. The parameterized view is defined like this:

```
CREATE VIEW blockbatch_gcs.read_date
AS SELECT
    chain,
    CAST(dt, 'Date') AS dt,
    *
FROM s3(
    concat(
        'https://storage.googleapis.com/oplabs-tools-data-sink/',
        {rootpath:String},
        '/chain=', {chain:String},
        '/dt=', {dt:String},
        '/*.parquet'
    )
    , '[HIDDEN]'
    , '[HIDDEN]'
    , 'parquet'
)
SETTINGS use_hive_partitioning = 1
```

For the `chain` and `dt` values to appear as top-level columns we need the `use_hive_partitioning`
setting. The view is a simple convenience so that users don't have to remember the exact syntax
for the `s3` table function and the credentials that are used to access the GCS bucket.

Here is how one would use the view to query the data:

```sql
SELECT
    *
FROM blockbatch_gcs.read_date(
    rootpath = 'blockbatch/account_abstraction/enriched_entrypoint_traces_v2',
    chain = 'base',
    dt = '2025-04-01'
)
LIMIT 10
SETTINGS use_hive_partitioning = 1
```

Notice that the `use_hive_partitioning` setting must be used both when creating the view and when
using the view in a query.


## Data Loading Jobs

Our platform supports loading data from GCS into ClickHouse, using data markers to make sure that
data is ready to load before executing the load job.


### Loading by Blockbatch

This section describes how to create a job that loads `blockbatch` data from GCS into ClickHouse
tables one block batch at a time. This can be useful if:

- You want to load data exactly AS-IS, without any transformations from GCS to ClickHouse. Only to
  gain the benefits of ClickHouse's performance and analytics capabilities.

- You want to load a filtered existing `blockbatch` model into ClickHouse. The results of a
  `blockbatch` model are large, so not practical to load into ClickHouse. In some cases filtering
  down (e.g. to a specific event or contract address) and loading the results into ClickHouse can
  be useful to power a specific analytics use case.

- You want to maintain a dimension table where the source of truth is the `blockbatch` model but
  the values in the dimension table are updated with each new block batch. A good example of this is
  the `dim_erc20_first_seen_v1` table which maintains a running `min(block_timestamp)` for ERC-20
  tokens.

Note that loading by blockbatch is generally not a good fit for aggregations. The loading process
will only be able to see the data for a single block batch at a time, so it will not be able to
aggregate on any useful grain. Advanced ClickHouse users will recognize that the destination table
in ClickHouse could have aggregation state columns, so it is definitely possible to use blockbatch
loading to compute aggregations. However it would be relatively complex and so we don't encourage
it.

To set up a blockbatch loading job all you need is the CREATE TABLE statement for the table you want
to load into, and the INSERT INTO statement that you want to use to load the data. We'll go over
that in more detail below.


### Loading by Date and Chain

There are cases where it is useful to operate on a date and chain grain instead of at the single
block batch level. The mental model you build about the data pipeline is much easier to reason
about since `chain` and `dt` map directly to the majority of our reports and dashboards.

Load jobs by `chain` ad `dt` are set up in a similar way to blockbatch loading jobs. You will need
the `CREATE` and `INSERT` statements that define the load job.

## Load Job Specification

There are four pieces you need to set up to create a load job:

1. The `CREATE TABLE` statement that defines the destination table.
2. The `INSERT INTO` statement that defines the source data.
3. The job specification which specifies the input data needed to run the job and the output
   table where the results will be written.
3. The `dagster` asset that defines the job

### Location

* Blockbatch load jobs: `src/op_analytics/datapipeline/etl/blockbatchload/ddl`.
* Date and chain load jobs: `src/op_analytics/datapipeline/etl/blockbatchloaddaily/ddl`.

### Naming convention

The `CREATE` and `INSERT` files are named after the table they are loading into and they have the
`__CREATE.sql` and `__INSERT.sql` suffixes to identify them.

For example here are the two files for the `create_traces_v1` table

```
src/op_analytics/datapipeline/etl/blockbatchload/ddl/
    contract_creation/create_traces_v1__CREATE.sql
    contract_creation/create_traces_v1__INSERT.sql
```

The root path of the output table is `blockbatch/contract_creation/create_traces_v1` and the
resulting table in ClickHouse will be `blockbatch.contract_creation__create_traces_v1` (the slash
in the root path is replaced with a double underscore).

A couple of points worth noting:

- In the `CREATE` file we use the `_placeholder_` table name. This gets replaced using the resulting
  table name derived from the naming convention.  We do this so that people don't have to remember
  to use the correct table name in the `CREATE` statement file.

- The `INSERT` file does not require the `INSERT INTO` portion, it is just a `SELECT` and the
  `INSERT` part is added by the system when it runs the job.

### Job Specification

The job specification is defined in the `datasets.py` file.  For batch and daily we use the following
python classes respectively:

- `op_analytics.datapipeline.etl.blockbatchload.loadspec.ClickHouseBlockBatchETL`
- `op_analytics.datapipeline.etl.blockbatchloaddaily.loadspec.ClickHouseDateChainETL`

The main goal for these classes is to be very explicit about the input datasets needed to produce
the output dataset. They also support configuration options that are specific to the type of job. 
Refer to the documentation for each of them in the code. 

## Data Readiness, Markers, and Job Idempotency

One of the trickiest parts of building data pipelines is triggering jobs. This amounts to answering
the question: when is the upstream data ready to be processed? To answer this question you need to
define the unit of incremental processing and to understand when data is complete (becomes immutable)
for that unit.

As mentioned before in this manual, our data platform uses "markers" to track execution of ingestion,
processing, and loading tasks. We leverage these markers to determine when data is ready to be
processed.

In the case of loading by batch this is simple. A batch is the unit of processing, and it is fully
ingested once we have written the four raw onchain data files to GCS for it (blocks, logs,
transactions, and traces). A blockbatch model is fully computed once the batch at the output root
path is written to GCS.

In the case of loading by date and chain this is more complex. We have to understand which batches
cover a given date, and track when all of them are ready to be processed. We use a simple heuristic.
We require that the batches for a give date do not have any block number gaps among them and that
we ingest the batches that straddle the start and end of the date (i.e. we have data for the previous
and the next dates). 

Using the readiness detection approaches we can ensure that we will never run a ClickHouse load job
on stale data. Before executing the load task the system check that the input data for the batch
(or chain/date) is ready. If it is not a warning is logged and the task is skipped. 

We also store markers for load tasks. So we can make sure that a give load task is executed only
once. If we already have the marker for a given task then we skip it on the next run of the 
scheduled job. Scheduled jobs always sweep a range of recent dates, that way they are always
attempting to load any pending tasks for which we don't have markers yet.

At the moment the only way to force a load task to execute more than once is by manually deleting
the corresponding marker from the marker store table in ClickHouse. That said we always assume
that the load job destination table is a ReplacingMergeTree that is configured to correctly handle
duplicate rows in case a load task is executed more than once. We do this because our markers approach
only guarantees at least once execution (and not exactly once).


## Building Data Pipelines

Each dataset defined as a `ClickHouseBlockBatchETL` or a `ClickHouseDateChainETL` can be thought
of as a node in a data pipeline. The collection of nodes that are chained together forms a data
pipeline.

The operation and maintenance of these blockbatch data pipelines relies on the following:

- No node is ever executed until the input data is ready.
- The go/pipeline Hex dashboard allows us to easily check if there is a problem with processing
  any of the batches.

### Execution

So far we have described how jobs are defined in SQL, but we need to execute them. The system
provides dedicated functions to execute by blockbatch and by date and chain jobs:

- `op_analytics.datapipeline.etl.blockbatchload.main.load_to_clickhouse`
- `op_analytics.datapipeline.etl.blockbatchloaddaily.main.daily_to_clickhouse`

Each of these functions takes a `dataset` argument which is the specification of the dataset to
load (specs are in the `datasets.py` files). 

The function also accepts a `range_spec` argument which is used to specify the date range that
should be loaded. In the case of loading by chain/date the function also lets you provide a `chains`
parameter to target only a subset of chains.

Both functions accept a `dry_run` parameter to allow you to test the job without actually loading
any data. When `dry_run=True` the SQL statement that will be executed is printed out but not 
sent to the database.

### Prototyping and Backfilling 

We use IPython notebooks to prototype and backfill data pipelines. Notebooks for blockbatch data
pipelines are located in the `notebooks/adhoc/blockbatch_clickhouse` directory. Browse that
directory for examples.

### Scheduling

We run the load jobs by block batch and by date and chain in two separate Dagster jobs. One job
targets all blockbatch datasets and the other targets all date and chain datasets.  The jobs are
group jobs (execute all the assets in a Dagster group) and each group is defined in a separate
assets python file:

- `src/op_analytics/dagster/assets/blockbatchload.py`
- `src/op_analytics/dagster/assets/blockbatchloaddaily.py`

The sequence of exactly what gets executed can be customized as needed within the Dagster asset
definition function. In most cases all you will need is to call the `load_to_clickhouse` or
`daily_to_clickhouse` function with the appropriate arguments.

### Monitoring

The loading by blockbatch and by date and chain tasks have dedicated marker tables in ClickHouse:

- `blockbatch_markers_datawarehouse`
- `blockbatch_daily`

These names are arguably not the best, but they are what we have. 

On top of these marker tables we have dedicated views in ClickHouse that make monitoring data
completeness easier:

- `etl_dashboard.blockbatch_load_markers_deduped`
- `etl_dashboard.blockbatch_load_markers_agged`
- `etl_dashboard.blockbatch_load_markers_status`
- `etl_dashboard.blockbatch_load_markers_missing` 

and for the date and chain load jobs:

- `etl_dashboard.blockbatch_daily_markers_deduped`
- `etl_dashboard.blockbatch_daily_markers_agged`
- `etl_dashboard.blockbatch_daily_markers_status`
- `etl_dashboard.blockbatch_daily_markers_missing`

We write markers every time a load task completes successfully, so if a task runs more than once we
have duplicates.  The `deduped` view helps query without duplicates. The `agged` view int turn
aggregates markers at the `root_path`, `dt`, and `chain` level so that it is easy to find out
which chains and dates are missing data. 

The `status` view brings in the data expectation (the batches that have been ingested) and then
find out which chains and dates have load tasks that have not been executed yet. This view shows
daily completion percentages for each date.

Finally the `missing` view gives us a raw list of batches that are missing. This view is used in
the "ALERTS" tab in the dashboard and should ideally be empty all the time. In the dashboard query
in Hex we set the delay threshold so that we only get alerts for batches that have been delayed by
longer than our latency SLA. 
