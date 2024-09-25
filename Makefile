
# Generate dbt sources from op_indexer schema definitions
schemas-to-dbt:
	uv run opdata dbt generate_sources

# Generate dbt docs
# NOTE (pedro - 2024/09/24) There is a bug in the "_get_one_catalog" function in dbt/adapters/base/impl.py
# For the duckdb adapter the get_one_catalog macro requires "needs_conn=True" but that kwarg is not set
# This causes the "docs generate" command to fail unless we use --empty-catalog.
dbt-docs: schemas-to-dbt
	cd dbt && uv run dbt --debug --macro-debugging docs generate --empty-catalog


# Generate and serve dbt docs
dbt-docs-serve: dbt-docs
	cd dbt && uv run dbt docs serve