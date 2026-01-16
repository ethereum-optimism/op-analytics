# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OP Analytics is a data engineering platform for the Optimism Superchain ecosystem. It aggregates data from multiple sources (Dune, L2Beat, DeFiLlama, Coingecko, GitHub, etc.), transforms it, and loads it into ClickHouse (primary data warehouse) and BigQuery for stakeholder dashboards.

## Common Commands

### Setup & Build
```bash
uv sync                    # Install dependencies
uv build                   # Build package
make uv-build              # Full build with dependency script
```

### Running Tests
```bash
pytest                     # Run all tests with coverage
pytest tests/op_analytics/datasources/dune/test_lookback_range.py  # Single test file
pytest -v --tb=short       # Verbose with short traceback
pytest --lf                # Run last failed tests only
```

### Linting & Formatting
```bash
ruff check src/ tests/     # Check for lint errors
ruff format src/ tests/    # Auto-format code
pre-commit run --all-files # Run all pre-commit hooks
```

### Running Dagster Locally
```bash
dagster dev                # Start local Dagster UI (usually http://localhost:3000)
```

### CLI
```bash
opdata --help              # Main CLI entry point
opdata dune --help         # Dune subcommand help
```

### Documentation
```bash
make html                  # Build Sphinx docs
make sphinx-serve          # Serve docs locally on port 8000
```

## Architecture

### Core Package Structure (`src/op_analytics/`)

- **cli/**: Typer-based CLI. Subcommands are auto-discovered from `cli/subcommands/` - each module must define an `app` Typer instance.

- **dagster/**: Orchestration layer. Assets defined in `dagster/assets/` are loaded by group name in `defs.py`. Key asset groups: blockbatchingest, blockbatchprocess, chainsdaily, defillama, dune, l2beat, growthepie, coingecko, github, governance, transforms.

- **datasources/**: External data integrations. Each datasource (e.g., `defillama/`, `l2beat/`, `coingecko/`) contains client code and Dagster asset definitions.

- **datapipeline/**: ETL processing with `etl/ingestion/` (data readers), `etl/loader/` (load strategies), and `etl/processor/` (transformations).

- **coreutils/**: Shared utilities including database clients (`clickhouse/`, `bigquery/`, `duckdb_inmem/`, `duckdb_local/`), logging (`logger.py`), storage abstractions, and partitioning logic.

- **transforms/**: SQL (Jinja2 templated) and Python data transformations organized by domain.

### Database Clients

- **ClickHouse**: Primary data warehouse. Client in `coreutils/clickhouse/`.
- **BigQuery**: For public datasets. Client in `coreutils/bigquery/`.
- **DuckDB**: In-memory (`duckdb_inmem/`) for local processing, file-based (`duckdb_local/`) for persistent local storage.

### Adding a New Data Source

1. Create `src/op_analytics/datasources/{source_name}/`
2. Implement data fetching in `client.py`
3. Define Dagster assets in `assets.py` with `@asset` decorators
4. Register the module name in `src/op_analytics/dagster/defs.py` MODULE_NAMES list
5. Add tests in `tests/op_analytics/datasources/{source_name}/`

## Code Style

- **Python**: 3.12+, ruff for linting/formatting, 100 char line limit
- **SQL**: sqlfluff with DuckDB dialect, Jinja2 templating, uppercase keywords, leading commas, 140 char line limit
- **Type checking**: mypy with `check_untyped_defs = true`

## Key Environment Variables

```
OP_CLICKHOUSE_HOST, OP_CLICKHOUSE_PORT, OP_CLICKHOUSE_USER, OP_CLICKHOUSE_PW
BQ_PROJECT_ID, GOOGLE_CLOUD_PROJECT
DUNE_API_KEY, GITHUB_API_TOKEN
```

## Docker & Deployment

```bash
make docker-image          # Build main Docker image
make docker-dagster        # Build and push Dagster image
make helm-dagster          # Deploy via Helm to Kubernetes
```

Kubernetes configs in `k8s/`, Helm charts in `helm/dagster/`.
