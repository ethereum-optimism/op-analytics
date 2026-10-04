# ClickHouseDataset

`ClickHouseDataset` is the successor of the [`DailyDataset`](./2-dailydata-gcs.md) from the
previous section. 

This data ingestion pattern was created as a realization that writing data in GCS is good for
portability (can read from both BigQuery and ClickHouse) but does not match the functionality
we can get by writing directly to ClickHouse, in particular

- The state of the ingestion table can be used to do incremental ingestion (only ingesting what we don't already have).
- The `ReplacingMergeTree` engine helps deduplicate input data.

## Subclassing the `ClickHouseDataset` class. 

This is the same as for the `DailyDataset` class. We also follow the same directory structure
convention where we have one directory and one `dataaccess.py` per datasource, for example:

```python
# FILE: src/op_analytics/datasources/governance/dataaccess.py

from op_analytics.coreutils.clickhousedata import ClickhouseDataset


class Governance(ClickhouseDataset):

    # Delegation events
    DELEGATE_CHANGED_EVENTS = "ingest_delegate_changed_events_v1"

    # Delegates
    DELEGATES = "ingest_delegates_v1"

    # Proposals
    PROPOSALS = "ingest_proposals_v1"
```

Each enum member corresponds to a table where data will be ingested. 

## CREATE TABLE

For each ingestion table defined in `dataaccces.py` you are expected to provide a corresponding
`CREATE TABLE` SQL file, which goes in the `ddl/` folder right beside `dataaccess.py`.

The file name should match the enum member value used in the `ClickHouseDataset` subclass:
```
src/op_analytics/datasources/governance/ddl
├── ingest_delegate_changed_events_v1.sql
├── ingest_delegates_v1.sql
├── ingest_proposals_v1.sql
├── ingest_votes_v1.sql
└── ingest_voting_power_snaps_v1.sql
```

The `ClickHouseDataset` class provides a `create_table()` function which locates the DDL and
runs it against the database. Most of the time users don't need to call `create_table()` directly
because the method is called every time `write()` is called.


## Writing Data

The `ClickHouseDataset` class has a `write()` method, very similar to the one in the `DailyDataset`
class. The method accepts a polars dataframe and will insert it to the destination table. For example:

```python
DaoPowerIndex.CPI_SNAPSHOTS.write(powerindex.snapshots_df)
```

Keep in mind that there will be cases when first going through a polars dataframe is not the best
approach. So you can always insert data directly into the ClickHouse table. The enum members
of a `ClickHouseDataset` subclass give you access to the db and table name strings so you can
use them as needed. 

If you want to you can even set up your own `INSERT INTO` statements in SQL files
and then coordinate execution of the statements from the `execute.py` file.

## Execution

Similar to what we do for `DailyDataset` data sources we have an execute.py file that has the
entrypoint function for the code that runs to ingest data. Here is an example:
```python
from .historical import HistoricalCPI
from .powerindex import ConcentrationOfPowerIndex
from .dataaccess import DaoPowerIndex


def execute_pull():
    powerindex = ConcentrationOfPowerIndex.fetch()
    historical = HistoricalCPI.fetch()

    DaoPowerIndex.CPI_SNAPSHOTS.write(powerindex.snapshots_df)
    DaoPowerIndex.CPI_COUNCIL_PERCENTAGES.write(powerindex.council_percentages_df)
    DaoPowerIndex.CPI_HISTORICAL.write(historical.df)
```

The business logic is isolated in separate dedicate modules (`historical.py`, `powerindex.py`)
and the prepared dataframes are written out using the `write()` method.


## Scheduling

This is the same as for `DailyDataset`. We have custom built Dagster assets that get assigned
to a specific scheduled job in our Dagster `defs.py`.


## Monitoring

Monitoring is an area where we can improve `ClickHouseDataset`. We don't use
markers here at the moment, so we don't have easy visibility of when data was written on a given
table. This is something that must be implemented in the future and will be important as new
data sources start adopting `ClickHouseDataset` over `DailyDataset`.
