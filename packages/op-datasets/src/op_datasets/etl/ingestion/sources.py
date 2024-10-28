import os
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

    # NOTE: Our data audits will fail if there are duplicates in the Goldsky tables.
    # If we manually verify that the data is correct when using a FINAL query then
    # we can run ingestion for the specific task with the GOLDSKY_FINAL set to true
    # to get the valid Goldsky data.
    if os.environ.get("GOLDSKY_FINAL") == "true":
        final_suffix = " FINAL"
    else:
        final_suffix = ""

    queries = {
        key: dataset.goldsky_sql(
            source_table=f"{block_batch.chain}_{dataset.goldsky_table_suffix}{final_suffix}",
            where=block_batch.filter(number_column=dataset.block_number_col),
        )
        for key, dataset in datasets.items()
    }

    def func(key):
        start = time.time()
        log.info(
            "Querying...",
            dataset=key,
            **block_batch.contextvars,
        )
        result = run_goldsky_query(queries[key])
        log.info(
            f"Query success {time.time() - start:.2f}s {len(result)} rows",
            dataset=key,
            **block_batch.contextvars,
        )
        return result

    return run_concurrently(func, list(queries.keys()), max_workers=-1)
