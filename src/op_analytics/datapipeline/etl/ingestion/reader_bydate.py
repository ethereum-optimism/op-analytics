from datetime import date
from typing import Iterable

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.partition import Partition
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.rangeutils.daterange import DateRange
from op_analytics.coreutils.time import surrounding_dates

from .reader_markers import (
    is_chain_active,
    IngestionDataSpec,
    DEFAULT_INGESTION_ROOT_PATHS,
    IngestionData,
)


log = structlog.get_logger()


def construct_readers_bydate(
    chains: list[str],
    range_spec: str,
    read_from: DataLocation,
    root_paths_to_read: list[str] | None = None,
) -> list[DataReader]:
    """Construct a list of DataReader for the given parameters.

    The parameters specify a set of chains, dates, and datasets that we are
    interested in processing.

    Each DataReader will be able to access the parquet data paths for the requested
    datasets (root_paths) on each date.

    Readers can be used for daily data processing.
    """
    date_range = DateRange.from_spec(range_spec)

    data_spec = IngestionDataSpec(
        chains=chains,
        root_paths_to_read=root_paths_to_read or DEFAULT_INGESTION_ROOT_PATHS,
    )

    markers_df = data_spec.query_markers(
        datevals=date_range.padded_dates(),
        read_from=read_from,
    )

    num_suspect = 0
    readers = []
    for dateval in date_range.dates():
        for chain in chains:
            if not is_chain_active(chain, dateval):
                log.info(f"skipping inactive chain: {str(dateval)} {chain} ")
                continue

            with bound_contextvars(chain=chain, date=dateval.isoformat()):
                filtered_df = markers_df.filter(
                    pl.col("chain") == chain,
                    pl.col("dt").is_in(surrounding_dates(dateval)),
                )

                # IMPORTANT: At this point the filtered_df contains data for more
                # dates than pertain to this task. This is so we can check data
                # continuity on the day before and after and determine if the input
                # is safe to consume.
                input_data = are_inputs_ready(
                    markers_df=filtered_df,
                    dateval=dateval,
                    root_paths_to_check=data_spec.root_paths_physical,
                    storage_location=read_from,
                )

                # Update data path mapping so keys are logical paths.
                dataset_paths = data_spec.data_paths(input_data.data_paths)

                obj = DataReader(
                    partitions=Partition.from_tuples(
                        [
                            ("chain", chain),
                            ("dt", dateval.strftime("%Y-%m-%d")),
                        ]
                    ),
                    read_from=read_from,
                    dataset_paths=dataset_paths,
                    inputs_ready=input_data.is_complete,
                )

                readers.append(obj)
                if not input_data.is_complete:
                    log.warning("MISSING DATA")
                    num_suspect += 1

    log.info(f"prepared {len(readers)} input batches.")
    if num_suspect > 0:
        log.info(f"input not ready for {num_suspect} batches.")

    return readers


def are_inputs_ready(
    markers_df: pl.DataFrame,
    dateval: date,
    root_paths_to_check: Iterable[str],
    storage_location: DataLocation,
) -> IngestionData:
    """Decide if the input data for a given date is complete.

    If the input data is complete, returns a map from root_path to list of parquet
    data paths for each root_path.

    If the input data is not complete returns None instead of the paths dict.
    """

    assert dict(markers_df.schema) == {
        "dt": pl.Date,
        "chain": pl.String,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "root_path": pl.String,
        "data_path": pl.String,
    }

    dataset_paths = {}

    for root_path in root_paths_to_check:
        dataset_df = markers_df.filter(pl.col("root_path") == root_path)

        dataset_ready = is_dataset_ready(
            root_path=root_path,
            dataset_df=dataset_df,
            dateval=dateval,
        )
        if not dataset_ready:
            return IngestionData(
                is_complete=False,
                data_paths=None,
            )

        parquet_paths = []
        for row in dataset_df.filter(pl.col("dt") == dateval).to_dicts():
            parquet_paths.append(storage_location.absolute(row["data_path"]))

        dataset_paths[root_path] = sorted(set(parquet_paths))

    return IngestionData(
        is_complete=True,
        data_paths=dataset_paths,
    )


def is_dataset_ready(
    root_path: str,
    dataset_df: pl.DataFrame,
    dateval: date,
) -> bool:
    with bound_contextvars(dataset=root_path):
        if dataset_df.is_empty():
            return False

        block_intervals = (
            dataset_df.select("min_block", "max_block", "dt").sort("min_block", "dt").to_dicts()
        )

        dates_covered = set()
        running_block = block_intervals[0]["max_block"]

        for block_interval in block_intervals:
            next_block = block_interval["min_block"]
            if next_block > running_block:
                log.warning(
                    "gap in block numbers",
                    gap_start=running_block,
                    gap_end=next_block,
                    gap_size=next_block - running_block,
                )
                return False

            running_block = block_interval["max_block"]
            dates_covered.add(block_interval["dt"])

        # Check that there is coverage from the day before the dateval
        # to the day after the dateval.
        expected = surrounding_dates(dateval)
        is_ready = sorted(dates_covered) == expected

        if not is_ready:
            missing = [_.isoformat() for _ in sorted(set(expected) - dates_covered)]
            log.warning("missing date coverage", missing=missing)

        return is_ready
