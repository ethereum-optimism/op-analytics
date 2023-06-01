# L2 Gas Price Tracker - Proof of Concept
This uses an ethereum-etl fork (WIP - normal ethereum-etl will also work) to stream transactions and blocks to a local postgres database. There is then a query that interprets this data to show the latest base fee, and create slow, medium, fast estimates for prioritiy fees. This output can then be shown in a regularly updating dashboard or frontend site.

- `transactions_initialize.sql`: SQL for generating the transactions table in postgres
- `blocks_initialize.sql`: SQL for generating the transactions table in postgres
- `optimism_etl_stream.ipynb`: Python notebook to run ethereum-etl streaming and post results to a database
- `gas_tracker_query.sql`: SQL query that pulls the last day of data and creates rolling gas price estimates. The most recent block in the result is the "current" recommendation.
- `l2_gas_estimator_output.csv`: Results of `gas_tracker_query.sql` in a csv.

This script also references `ethereumetl_utils.py` which can be found in op-analytics/helper-functions.
