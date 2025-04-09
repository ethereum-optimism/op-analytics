# General Purpose Data Pipelines: Transforms

In the previous section we went over how to build data pipelines that process onchain data using
ClickHouse as the compute engine. In this section we talk about data pipelines for a wider variety
of use cases.

The "transforms" system was developed before we built the blockbatch load system and so we also use
it for onchain data (e.g. the interop transforms group). That said it would be a good idea to
migrate some of the onchain data tables from the transforms system to the blockbatch system, since
the latter has better data integrity guarantees and better monitoring.

## Transform Groups

A transform group is a collection of tables and views that are processed together. You can think
of a group as a mini-pipeline where a series of tables are populated one after the other.

Let's take as an example the `interop` transform group. This group has the following ClickHouse
tables:

- `dim_erc20_with_ntt_first_seen_v1`
- `fact_erc20_oft_transfers_v1`
- `fact_erc20_ntt_transfers_v1`
- `dim_erc20_ntt_first_seen_v1`
- `dim_erc20_oft_first_seen_v1`
- `fact_erc20_create_traces_v2`
- `export_fact_erc20_create_traces_v2`

## Transform Specification

Each table in a transform group is defined by a `CREATE` statement that defines the table schema
and ClickHouse engine (including the very important `ORDER BY` clause).  And an `INSERT` statement
that will be used to populate the table.

### Directory Structure and Naming Convention

The directory structure for a transform group consists of two folders:

- `create/`: Has the `CREATE` statements for all the tables in the group.
- `insert/`: Has the `INSERT` statements for all the tables in the group.

The naming convention for files in each of the folders is the following:

- `[INDEX]_[tablename].sql

Where `[INDEX]` is a number that indicates the order of execution and `[tablename]` is the name of
the table.

Let's take the `fees` transforms croup as an example:
```
src/op_analytics/transforms/fees
├── create
│   ├── 01_agg_daily_transactions_grouping_sets.sql
│   ├── 02_agg_daily_transactions.sql
│   ├── 10_view_agg_daily_transactions_tx_from_tx_to_method.sql
│   ├── 11_view_agg_daily_transactions_tx_to_method.sql
│   └── 12_view_agg_daily_transactions_tx_to.sql
└── update
    ├── 01_agg_daily_transactions_grouping_sets.sql
    └── 02_agg_daily_transactions.sql
```

This group has 2 tables and 3 views.  For each of the two tables we have a corresponding `update`
SQL statement that is used to populate the table.

Note that it is prefectly fine to have any kind of SQL file in the `create` folder. Although it's
better not to get too creative. If you inspect the SQL files for the `views` you will see that they
are stright-up `CREATE VIEW` ClickHouse statements.


## Transform Execution Model

The execution model for a transform is very simple:

- When the system runs a transforms it first runs all the `CREATE` statements in order accodring to
  their index.

- It then runs all the `INSERT` statements in order according to their index.

- The system provides the execution date as a query parameter (`dt`) that may or may not be
  utilized by the `INSERT` statement.


## Building Data Pipelines

To build a `transforms` pipeline all you need to do is create a new directory in the `src/op_analytics/transforms`
directory and add the appropriate SQL files.


### Execution

We provide a function to execute a transform group for a given date range:

- `src.op_analytics.transforms.main.execute_dt_transforms`

For a detailed description of the parameters see the function's docstring.

### Prototyping and Backfilling

The `execute_dt_transforms` function is used from an IPython notebook to prototype and also to
manually backfill data.  Notebooks for transforms are located in the
`notebooks/adhoc/clickhouse_transforms` directory. Browse that directory for examples.

### Scheduling


All transforms-related Dagster assets are defined in the `src/op_analytics/dagster/assets/transforms.py` file.
There is no hard and fast rule for how to define the assets. Generally we schedule one asset per
group, but there are cases where we may want to have more control over what is executed on each
day and so each assest can decide how it calls the `execute_dt_transforms` function.

Similarly for Dagster jobs, there is no hard and fast rule but we generally define one scheduled
job per transform group.  The jobs are defined in the `src/op_analytics/dagster/defs.py` file.


### Markers

We use markers to track the execution of the transforms. The markers are stored in the
`etl_monitor.transform_dt_markers` table in ClickHouse.


### Monitoring

Unfortunately we have not yet built monitoring tools for the transforms system. I have talked about
the idea of having `quality` tables as part of a transforms group. A `quality` table is a table that
contains information about anything that might be wrong with the data in the group.

