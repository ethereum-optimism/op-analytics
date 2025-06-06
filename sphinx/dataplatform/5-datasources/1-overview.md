# Overview

## DataAccess Pattern

When we ingest data from a third-party system the logic for how to obtain the data and how to
transform it before writing it out varies for each use case.  However, once we have a dataframe
with the resulting data the way in which we write out to our data lake should be the same
across all use cases.

Aside of this it is also important to make it easy for the team to find where is the code
that is used to ingest data for a specific data source. 


## Directory Structure

To achieve discoverability we require data sources to adhere to a directory structure. 
A `datasource` is a collection of related tables that are ingested.  The `datasource` has 
a dedicated directory and inside that directory we have  e a `dataacccess.py` file where
an enumeration of the data source tables can be found.

At the time of writing this looks like this in the op-analytics repo:
```
src/op_analytics/datasources
├── chainsmeta
│   └── dataaccess.py
├── daopowerindex
│   └── dataaccess.py
├── defillama
│   └── dataaccess.py
├── dune
│   └── dataaccess.py
├── github
│   └── dataaccess.py
├── governance
│   └── dataaccess.py
├── growthepie
│   └── dataaccess.py
├── l2beat
    └── dataaccess.py
```

The enumeration of tables defined in the `dataaccess.py` file can be one of two kinds:

- `DailyDataset`: Data ingested to GCS and partitioned by `dt`.
- `ClickhouseDataset`: Data ingested to our ClickHouse data warehouse.

The following sections explain each of these two kinds and their capabilities.