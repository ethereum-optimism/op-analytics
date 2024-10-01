from op_coreutils.clickhouse.client import run_queries_concurrently
from op_coreutils.logger import LOGGER

from op_datasets.coretables.blockrange import BlockRange
from op_datasets.schemas.blocks import BLOCKS_SCHEMA
from op_datasets.schemas.transactions import TRANSACTIONS_SCHEMA

log = LOGGER.get_logger()


def jinja(val: str):
    return "{{ " + val + " }}"


SCHEMAS = {
    "blocks": BLOCKS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
}


def get_sql(chain_name: str, dataset: str, use_dbt_ref: bool = False, filter: str | None = None):
    schema = SCHEMAS[dataset]

    exprs = [
        "    " + _.op_analytics_clickhouse_expr
        for _ in schema.columns
        if _.op_analytics_clickhouse_expr is not None
    ]
    cols = ",\n".join(exprs)

    table = (
        jinja(f'source("superchain_goldsky", "{chain_name}_{dataset}")')
        if use_dbt_ref
        else f"{chain_name}_{dataset}"
    )
    if filter is None:
        return f"SELECT\n{cols}\nFROM {table}"
    else:
        return f"SELECT\n{cols}\nFROM {table} WHERE {filter}"


def get_core_tables(blocks: str):
    """Get the core dataset tables from Goldsky."""
    block_range = BlockRange.from_str(blocks)

    names = ["blocks", "transactions"]
    queries = [
        get_sql("op", "blocks", filter=block_range.filter()),
        get_sql("op", "transactions", filter=block_range.filter(number_column="block_number")),
    ]
    dataframes = run_queries_concurrently(queries)

    return {names[i]: df for i, df in enumerate(dataframes)}
