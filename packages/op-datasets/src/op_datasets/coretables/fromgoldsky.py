import time

import polars as pl
from op_coreutils.clickhouse.client import run_query
from op_coreutils.logger import structlog
from op_coreutils.threads import run_concurrently

from op_datasets.processing.blockrange import BlockRange
from op_datasets.schemas import CoreDataset


log = structlog.get_logger()


def read_core_tables(
    chain: str, datasets: dict[str, CoreDataset], block_range: BlockRange
) -> dict[str, pl.DataFrame]:
    """Get the core dataset tables from Goldsky."""

    queries = {
        key: dataset.goldsky_sql(
            source_table=f"{chain}_{dataset.goldsky_table}",
            where=block_range.filter(number_column=dataset.block_number_col),
        )
        for key, dataset in datasets.items()
    }

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
