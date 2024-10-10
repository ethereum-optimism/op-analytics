import os
from datetime import date, timedelta

from op_coreutils.logger import structlog

from op_datasets.processing.blockrange import BlockRange
from op_datasets.processing.ozone import (
    construct_dataset_path,
    construct_date_path,
    construct_parquet_filename,
    construct_parquet_path,
)
from op_datasets.schemas import CoreDataset

import polars as pl

log = structlog.get_logger()


def read_core_tables(
    base_path: str, chain: str, datasets: dict[str, CoreDataset], block_range: BlockRange
):
    """Get the core dataset tables from a local directory."""

    blocks_dataset = datasets["blocks"]

    blocks_path = os.path.join(base_path, construct_dataset_path(blocks_dataset.name, chain))
    dates = [_.removeprefix("dt=") for _ in os.listdir(blocks_path)]

    log.debug(f"Locating parquet files for blocks {block_range.min} to {block_range.max}")

    search_filename = construct_parquet_filename(block_range)

    def lookup(dt):
        date_path = os.path.join(base_path, construct_date_path(blocks_dataset.name, chain, dt))

        if not (os.path.exists(date_path) and os.path.isdir(date_path)):
            return None

        date_parquet_files = sorted(os.listdir(date_path))

        if search_filename in date_parquet_files:
            return 0
        if search_filename < date_parquet_files[0]:
            return -1
        if search_filename > date_parquet_files[-1]:
            return 1

    # Find a date that includes the block range.
    search_result = binary_search(lookup, dates)

    # The block range may be split across multiple dates.
    matching_dates = [search_result]

    # Look 1 day before and after to see if part of the block range is present.
    result_date = date.fromisoformat(search_result)
    previous_date = (result_date - timedelta(days=1)).strftime("%Y-%m-%d")
    next_date = (result_date + timedelta(days=1)).strftime("%Y-%m-%d")

    if lookup(previous_date) == 0:
        matching_dates.append(previous_date)
    if lookup(next_date) == 0:
        matching_dates.append(next_date)

    # Read parquet files
    dataframes = {}
    for key, dataset in datasets.items():
        dataset_dfs = []
        for dt in sorted(matching_dates):
            parquet_path = os.path.join(
                base_path, construct_parquet_path(dataset.name, chain, dt, block_range)
            )
            dataset_dfs.append(
                pl.read_parquet(parquet_path).with_columns(chain=pl.lit(chain), dt=pl.lit(dt))
            )

        dataframes[key] = pl.concat(dataset_dfs)

    return dataframes


def binary_search(lookup, items):
    candidate_idx = len(items) // 2
    candidate = items[candidate_idx]
    lookup_result = lookup(candidate)

    log.debug(
        f"Binary search in {len(items)} items [{items[0]} ... {items[-1]}], result = {lookup_result}"
    )

    if lookup_result == 0:
        return candidate
    elif lookup_result < 0:
        return binary_search(lookup, items[:candidate_idx])
    elif lookup_result > 0:
        return binary_search(lookup, items[candidate_idx:])
    elif lookup_result is None:
        raise RuntimeError("should not get here.")
