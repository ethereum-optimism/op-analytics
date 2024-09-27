#!/bin/bash

set -o allexport
source ".env"
set +o allexport

cd dbt || exit
uv run dbt --debug --macro-debugging docs generate --profile clickhouse_goldsky
