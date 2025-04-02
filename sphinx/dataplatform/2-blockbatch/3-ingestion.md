# Ingestion


[Goldsky](https://docs.goldsky.com/) is our current raw onchain data source. We get data from Goldsky
through a shared ClickHouse database. The streaming Goldsky pipelines write data directly to the
database. This means that these shared tables are exposed to any possible data quality issues that
Goldsky runs into. This includes things like gaps in block numbers, missing traces, missing logs, and
reorged blocks or transactions.

The main goal of our ingeston process is to audit the data in the  shared ClickHouse instance and
only ingest it into our system if it passes our audits. Usually once every two weeks or so we reach
out to the Goldsky team to get help with a data delay or data integrity issue. Our system flags the
problem which prevents our ingestion pipeline form making progress. We then reach out to Goldsky
and after they fix the problem on their end, which they do very quickly, our pipeline can resume.

Reading large amounts of data from the shared Goldsky ClickHouse instance is slow and incurs network
costs. So our ingestion process also helps make a copy of the data in our own GCS bucket which makes
it faster and cheaper to access for any of our downstream procesisng.

Refer to 
`src/op_analytics/datapipeline/etl/ingestion/audits/audits.py`
for more details on the audits that are run on each block batch.

