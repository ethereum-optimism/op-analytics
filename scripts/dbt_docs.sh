#!/bin/bash


cd dbt || exit

uv run opdata misc run_with_env  \
  "CLICKHOUSE_GOLDSKY_DBT_HOST" "CLICKHOUSE_GOLDSKY_DBT_PORT" "CLICKHOUSE_GOLDSKY_DBT_USER" "CLICKHOUSE_GOLDSKY_DBT_PASSWORD" \
  "dbt --debug --macro-debugging docs generate --profile clickhouse_goldsky"

