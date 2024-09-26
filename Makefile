.PHONY: sphinx-docs
sphinx-docs: docs/sphinx/html/index.html
	$(MAKE) -C sphinx clean
	$(MAKE) -C sphinx html


sphinx-serve: sphinx-docs
	python -m http.server


docs/sphinx/html/index.html:
	$(MAKE) -C sphinx clean
	$(MAKE) -C sphinx html

.PHONY: dbt-docs
dbt-docs: docs/dbt/index.html


.PHONY: dbt-docs-open
dbt-docs-open: docs/dbt/index.html
	open docs/dbt/index.html
	

# Generate dbt sources from op_indexer schema definitions
dbt/sources/indexed.yml:
	uv run opdata dbt generate_sources


# Generate dbt docs
#
# NOTE (pedro - 2024/09/24) There is a bug in the "_get_one_catalog" function in dbt/adapters/base/impl.py
# For the duckdb adapter the get_one_catalog macro requires "needs_conn=True" but that kwarg is not set
# This causes the "docs generate" command to fail unless we use --empty-catalog.
#
# The second command here is to customize the docs site and bundle it up as a single static HTML file.
docs/dbt/index.html: dbt/sources/indexed.yml docs/dbt/optimism.css $(wildcard dbt/docs/*.md)
	cd dbt && uv run dbt --debug --macro-debugging docs generate --empty-catalog
	uv run opdata dbt docs_custom
