import time
from enum import Enum

import polars as pl
from op_coreutils.clickhouse import run_goldsky_query
from op_coreutils.logger import structlog
from op_coreutils.threads import run_concurrently

from op_datasets.schemas import CoreDataset

from .batches import BlockBatch

log = structlog.get_logger()


class RawOnchainDataProvider(str, Enum):
    """Providers of raw onchain data."""

    GOLDSKY = "GOLDSKY"


def read_from_source(
    provider: RawOnchainDataProvider,
    datasets: dict[str, CoreDataset],
    block_batch: BlockBatch,
):
    if provider == RawOnchainDataProvider.GOLDSKY:
        return _read_from_goldsky(datasets, block_batch)

    raise NotImplementedError(f"raw data provider is not implemented: {provider}")


def _read_from_goldsky(
    datasets: dict[str, CoreDataset],
    block_batch: BlockBatch,
) -> dict[str, pl.DataFrame]:
    """Get the core dataset tables from Goldsky."""

    queries = {
        key: dataset.goldsky_sql(
            source_table=f"{block_batch.chain}_{dataset.goldsky_table_suffix}",
            where=block_batch.filter(number_column=dataset.block_number_col),
        )
        for key, dataset in datasets.items()
    }

    def func(key):
        start = time.time()
        try:
            result = run_goldsky_query(queries[key])
            log.info(f"Query success: {key} in {time.time() - start:.2f}s {len(result)} rows")
            return result
        except Exception:
            log.error(f"Query failure: {key}: \n{queries[key]}")
            raise

    return run_concurrently(func, list(queries.keys()), 4)
