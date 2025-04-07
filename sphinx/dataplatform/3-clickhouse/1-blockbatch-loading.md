# Blockbatch Data Loading

The data produced by `blockbatch` processing is stored in GCS, so it is not easily accessible for
dashboards and SQL-based data pipelines. In this section we go over how to explore and prototype
with `blockbatch` data in GCS, and how to create jobs that load the data into ClickHouse to power
downstream analytics and dashboards.


## Ad-hoc Blockbatch Queries

We have set up a parameterized view on our ClickHouse instance that allows you to query data from
a blockbatch root path for a given chain and date. The parameterized view is defiend like this:

```sql
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

For the `chain` and `dt` valus to appear as top-level columns we need the `use_hive_partitioning`
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

- You want to load a filtered existing `blockbatch` model into ClickHoust. The results of a
  `blockbatch` model are large, so not practical to load into ClickHouse. In some cases filtering
  down (e.g. to a specific event or contract address) and loading the results into ClickHouse can
  be useful to power a specific analytics use case.

- You want to maintain a dimension table where the source of truth is the `blockbatch` model but
  the values in the dimension table are updated with each new block batch. A good example of this is
  the `dim_erc20_first_seen_v1` table which maintians a running `min(block_timestamp)` for ERC-20
  tokens.

Note that loading by blockbatch is generally not a good fit for aggregations. The loading process
will only be able to see the data for a single block batch at a time, so it will not be able to
aggregate on any useful grain. Advanced ClickHouse users will recognize that the destination table
in ClickHouse could have aggreagation state columns, so it is definitely possible to use blockbatch
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

- `ClickHouseBlockBatchDataset`
- `ClickHouseDailyDataset`

The main point of this classes is the be very explicit about the input data needed to run the job.
They also support configuration options that are specific to the type of job. Please refer to the
documentation for each of these classes for more details.

### Markers, Data Quality, and Job Idempotency

..TODO..
