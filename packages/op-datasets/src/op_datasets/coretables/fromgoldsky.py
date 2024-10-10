import time

import polars as pl
from op_coreutils.clickhouse.client import run_query
from op_coreutils.logger import structlog
from op_coreutils.threads import run_concurrently

from op_datasets.processing.blockrange import BlockRange
from op_datasets.schemas import CoreDataset


log = structlog.get_logger()


def jinja(val: str):
    return "{{ " + val + " }}"


def get_sql(
    chain_name: str,
    dataset: CoreDataset,
    block_range: BlockRange | None = None,
    use_dbt_ref: bool = False,
):
    exprs = [
        "    " + _.op_analytics_clickhouse_expr
        for _ in dataset.columns
        if _.op_analytics_clickhouse_expr is not None
    ]
    cols = ",\n".join(exprs)

    table = (
        jinja(f'source("superchain_goldsky", "{chain_name}_{dataset.goldsky_table}")')
        if use_dbt_ref
        else f"{chain_name}_{dataset.goldsky_table}"
    )
    if block_range is None:
        return f"SELECT\n{cols}\nFROM {table}"
    else:
        filter = block_range.filter(number_column=dataset.block_number_col)
        return f"SELECT\n{cols}\nFROM {table} WHERE {filter}"


def read_core_tables(
    chain: str, datasets: dict[str, CoreDataset], block_range: BlockRange
) -> dict[str, pl.DataFrame]:
    """Get the core dataset tables from Goldsky."""

    queries = {key: get_sql(chain, dataset, block_range) for key, dataset in datasets.items()}

    def func(key):
        start = time.time()
        try:
            result = run_query(queries[key])
            log.info(f"Query success: {key} in {time.time() - start:.2f}s {len(result)} rows")
            return result
        except Exception:
            log.error(f"Query failure: {key}: \n{queries[key]}")
            raise

    return run_concurrently(func, list(queries.keys()), 4)
