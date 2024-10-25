from datetime import date

import polars as pl
from op_coreutils.logger import structlog
from op_coreutils.time import surrounding_dates

from op_datasets.etl.ingestion.utilities import RawOnchainDataLocation

log = structlog.get_logger()


def are_inputs_ready(
    markers_df: pl.DataFrame,
    dateval: date,
    expected_datasets: set[str],
    storage_location: RawOnchainDataLocation,
) -> dict[str, list[str]] | None:
    """Decide if we the input data for a given date is complete.

    If the input data is complete, returns a map from datset to list of parquet paths that
    contain data for each dataset.

    If the input data is not complete returns None.
    """

    assert markers_df.schema == {
        "dt": pl.Date,
        "chain": pl.String,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "dataset_name": pl.String,
        "data_path": pl.String,
    }

    datasets = set(markers_df["dataset_name"].unique().to_list())

    if datasets != expected_datasets:
        for name in expected_datasets - datasets:
            log.warning(f"input data is not complete for {name!r}")
        return None

    dataset_paths = {}
    for dataset in datasets:
        dataset_df = markers_df.filter(pl.col("dataset_name") == dataset)

        dataset_ready = is_datset_ready(
            dataset_df=dataset_df,
            dateval=dateval,
        )
        if not dataset_ready:
            return None

        parquet_paths = []
        for row in dataset_df.filter(pl.col("dt") == dateval).to_dicts():
            parquet_paths.append(storage_location.absolute(row["data_path"]))

        dataset_paths[dataset] = sorted(parquet_paths)

    # If we get to this point then data is ready to be consumed!
    return dataset_paths


def is_datset_ready(dataset_df: pl.DataFrame, dateval: date) -> bool:
    block_intervals = (
        dataset_df.select("min_block", "max_block", "dt").sort("min_block", "dt").to_dicts()
    )

    dates_covered = set()
    running_block = block_intervals[0]["max_block"]

    for block_interval in block_intervals:
        next_block = block_interval["min_block"]
        if next_block > running_block:
            log.warning(
                f"Detected a gap in block numbers: jumps from:#{running_block} to #{next_block}, gap={next_block - running_block}"
            )
            return False

        running_block = block_interval["max_block"]
        dates_covered.add(block_interval["dt"])

    # Check that there is coverage from the day before the dateval
    # to the day after the dateval.
    return sorted(dates_covered) == surrounding_dates(dateval)
