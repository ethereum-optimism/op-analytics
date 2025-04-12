# OP Labs Public BigQuery Data

The OP Labs Data Team makes onchain datasets available to the wider community as BigQuery Analytics
Hub listings. In this section we describe the process we use to load data from our GCS data lake
into BigQuery.

At the time of writing the only data we make public is data coming out of our 
[blockbatch processing](../2-blockbatch/index) data pipeline. Also all datasets are partitioned by date
and bucketed by chain.


## Data Readiness

To avoid duplicate rows in BigQuery we must update an entire `dt` partition (including data for all
chains) all at once each time we load. So the difficult part of the load process is figuring out
when all data for all chains on a given `dt` has been processed and is ready to load. To do this
we rely on the blockbatch processing [markers](../2-blockbatch/2-markers) that keep track of
what has been processed for each root path. 

## Loading Function

The general purpose loading function is `load_blockbatch_to_bq` defined in 
`src/op_analytics/datapipeline/etl/loadbq/load.py`. This function lets us specify the root paths
that will be loaded into BigQuery and the destination BigQuery dataset. We load each root path 
to a corresponding table in the BigQuery dataset.

The `load_blockbatch_to_bq` takes care of checking readings for the blockbatch data at the
specified root paths. 

To submit the BigQuery load job the markers are used to find out the exact GCS URIs for the parquet
files that comprise the data for all chains on a given `dt`. This list of URIs is the only thing
we need to submit the load job, and BigQuery takes it from there.

This approach is very cost effective because loading data into BigQuery from GCS using load jobs
is a free operation. Users of the public lists pay for compute out of their own Google Cloud
accounts.


## Datasets

### Raw Onchain Data ([listing URL](https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/523274563936/locations/us/dataExchanges/optimism_192d403716e/listings/optimism_superchain_raw_onchain_data_192fdc72e35))

Raw on-chain data for the most prominent superchain chains is made available. This includes the
following tables:

- `blocks`
- `logs`
- `transactions`
- `traces`

### Account Abstraction ([listing URL](https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/523274563936/locations/us/dataExchanges/optimism_192d403716e/listings/optimism_superchain_4337_account_abstraction_data_1954d8919e1))

OP Labs maintains account abstraction datasets for superchain chains. This includes the following
tables:

- `useroperationevent_logs_v2`: Decoded `UserOperationEvent` logs.
- `enriched_entrypoint_traces_v2`: Decoded  `innerHandleOp` traces joined with `UserOperationEvent` logs.

More documentation for the Account Abstraction pipeline can be found in the [README](https://github.com/ethereum-optimism/op-analytics/tree/c8ca13f55b4ea8868c49c288b1a87ad0e6e0a095/src/op_analytics/datapipeline/models/code/account_abstraction#readme) file.

