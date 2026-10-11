# Design Principles


The `blockbatch` data processing pattern is designed around the idea of deterministic parallelism.
For a given blockchain we should be able to unambiguously determine in advance how it will be broken
up  into batches for processing.

This has two benefits.  First, it allows us to construct processing tasks that can be run in parallel
without having to look at the data. Second, the storage location of any processed results is also
deterministic. So if we need to reprocess a batch, we will be sure that there won't be no duplicates
at the storage layer.

Blockchains can vary widely in transaction activity, and also a given blockchain can
explode in popularity increasing the amount of data that needs to be processed per block.
So for `blockbatch` processing we need a way to
configure the batch size based on the chain's characteristics. This method should allow adjusting
the batch size so that as a chain grows we can keep the amount of data per batch roughly constant.
In `blockbatch` processing a single batch is processed by a single worker. So we should aim for
batch sizes that fit well into a single worker's memory.

The main trade-off of this approach is that it requires the pipeline owner to be aware of batch
sizes so they can be configured in advance, budgeting for any increase in data volume. Usually this
is not much work, but there can be scenarios where a chain explodes in popularity and the pipeline
might stall. If that happens one can always pause the pipeline and back-date an adjustment to
the batch size.


## Batch Size Configuration

In the snippet below we show how we have configured the batch sizes for `base`:

```python
BATCH_SIZE_CONFIGURATION = {
    "base": [
        Delimiter(block_number=0, batch_size=10000),
        Delimiter(block_number=10000000, batch_size=5000),
        Delimiter(block_number=15000000, batch_size=2000),

        # Starting here base is ~2.5M traces / 1k blocks.
        # Traces file can be ~70MB.
        Delimiter(block_number=20900000, batch_size=1000),

        # (2024/10/29) Decided to go to 400 blocks per batch to give us more room.
        # Traces file size is ~25MB.
        Delimiter(block_number=21200000, batch_size=400),

        # (2025/03/17) Go to 200 blocks per batch to plan for the future.
        # Trace file size is at ~55MB.
        Delimiter(block_number=27730000, batch_size=200),
    ],
}
```

We see how the batch size is configured with a list of `Delimiter` objects. Each delimiter
tells us what the batch size should be starting at the boundary block number and going forward.
To adjust the batch size we add new delimiters to the list.

Delimiters are like feature flags that kick in at a point in time. When we add them we don't want
to change any processing that is currently in flight, so we make sure that a new delimiter has a
block number that is sometime in the future, say a couple of days after the change is deployed.
That way nothing changes in the present and when the chain gets to the new delimiter block number
then the batch size will transition to its new value.


## Data Artifacts and Storage Location

Going back to our principle of determinism, in `blockbatch` processing the storage location of
produced data should be unambiguously determined from the block batch. The storage layer we use
is Google Cloud Storage (GCS) and we introduce the concept of a `root_path` to identify the
different data artifacts associated with a batch.

The most important data artifact is the raw data itself. EVM blockchains produce four key datasets,
and for each of these we have a different root path in our storage bucket:

- Blocks -> `ingestion/blocks_v1`
- Transactions -> `ingestion/transactions_v1`
- Logs -> `ingestion/logs_v1`
- Traces -> `ingestion/traces_v1`

For a given batch we can compute many other data artifacts. For example transaction fee details,
filtered contract creation traces, decoded 4337 activity, etc. The possibilities are endless. Each
data artifact gets its own `root_path` but shares a location for the batch based on the batch block
range.

For example, the raw traces for the OP Mainnet batch with blocks 600000 to 610000 are located at:
```
gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=op/dt=2021-11-30/000000600000.parquet
```

And the filtered contract creation traces for the same batch are located at:
```
gs://oplabs-tools-data-sink/blockbatch/contract_creation/create_traces_v1/chain=op/dt=2021-11-30/000000600000.parquet
```

## Date Partitioning

The paths shown in the above example have a `dt` component that is the date in YYYY-MM-DD format.
This is an subtle but important detail, it in fact makes the path of the data not be completely
deterministic. For a given block we need to know then date on which it will be produced, which is
not known until the block is actually produced.

Date partitioning is introduced with the goal of making it easier to reason about the data as a time
series. This is very important for the kinds of analytics we do at OP Labs. Pretty much all of our
dashboards and reports have a date component on them.

The price we pay for having the date as a partition is we cannot do O(1) data access given the block
number. To access the data for a block we need to first find the block timestamp from which we
can find the date partition. Then with the batch size configuration we can find the full URI for
the block batch that contains the block.

This limitation turns out not to be a huge deal, because we can bootstrap knowledge of the date
partition at ingestion time and then carry it forward in all our metadata from that point on.

## Partitioning at Ingestion

When ingesting data we can read from the data source (in our case this is Goldsky but can also be
and RPC provider) targeting the blocks we need using the min/max block number. We proceed
to audit the batch and then right before writing we split the batch into date partitions using
the block timestamp for each block to determine which date does it go into.

Most of the time there is a single date per batch, but when a batch straddles a date boundary we
end up with two dates and therefore two parquet files written to the data lake, for example

```
gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=op/dt=2021-11-30/000000630000.parquet
gs://oplabs-tools-data-sink/ingestion/traces_v1/chain=op/dt=2021-12-01/000000630000.parquet
```

When processing data after ingestion we don't usually address the single batch, but rather we
address all batches for a given date range. So we can use the list of files created at ingestion
to find out what data needs to be processed.












