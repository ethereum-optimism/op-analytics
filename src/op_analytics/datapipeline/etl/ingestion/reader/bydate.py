from datetime import date
from typing import Iterable

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.partition import Partition
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.time import surrounding_dates
from op_analytics.datapipeline.chains.activation import is_chain_active, is_chain_activation_date

from .request import BlockBatchRequest, BlockBatchRequestData

log = structlog.get_logger()


def construct_readers_bydate(
    blockbatch_request: BlockBatchRequest,
    read_from: DataLocation,
) -> list[DataReader]:
    """Construct a list of DataReader for the given parameters.

    The parameters specify a set of chains, dates, and datasets that we are
    interested in processing.

    Each DataReader will be able to access the parquet data paths for the requested
    datasets (root_paths) on each date.

    Readers can be used for daily data processing.
    """

    # We request 1-day padded dates so that we can use the query results to
    # check if there is data on boths ends. This allows us to confirm that the
    # data is ready to be processed.
    markers_df = blockbatch_request.query_markers(location=read_from, padded_dates=True)

    datevals = blockbatch_request.time_range.to_date_range().dates()
    num_suspect = 0
    readers = []

    for dateval in datevals:
        for chain in blockbatch_request.chains:
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
                    chain=chain,
                    dateval=dateval,
                    root_paths_to_check=blockbatch_request.physical_root_paths_for_chain(chain),
                    storage_location=read_from,
                )

                # Update data path mapping so keys are logical paths.
                dataset_paths: dict[str, list[str]] = (
                    blockbatch_request.data_paths_keyed_by_logical_path(
                        chain,
                        input_data.data_paths,
                    )
                )

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
                    extra_marker_data={},  # No extra marker data on a bydate reader.
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
    chain: str,
    dateval: date,
    root_paths_to_check: Iterable[str],
    storage_location: DataLocation,
) -> BlockBatchRequestData:
    """Decide if the input data for a given date is complete.

    If the input data is complete, returns a map from root_path to list of parquet
    data paths for each root_path.

    If the input data is not complete returns None instead of the paths dict.
    """

    assert dict(markers_df.schema) == {
        "dt": pl.Date,
        "chain": pl.String,
        "marker_path": pl.String,
        "num_parts": pl.UInt32,
        "num_blocks": pl.Int32,
        "min_block": pl.Int64,
        "max_block": pl.Int64,
        "root_path": pl.String,
        "data_path": pl.String,
    }

    dataset_paths = {}

    # The activation date does not require verifying data on the prior day.
    is_activation = is_chain_activation_date(chain, dateval)

    for root_path in root_paths_to_check:
        dataset_df = markers_df.filter(pl.col("root_path") == root_path)

        dataset_ready = is_dataset_ready(
            root_path=root_path,
            dataset_df=dataset_df,
            dateval=dateval,
            is_activation_date=is_activation,
        )
        if not dataset_ready:
            return BlockBatchRequestData(
                is_complete=False,
                data_paths=None,
            )

        parquet_paths = []
        for row in dataset_df.filter(pl.col("dt") == dateval).to_dicts():
            parquet_paths.append(storage_location.absolute(row["data_path"]))

        dataset_paths[root_path] = sorted(set(parquet_paths))

    return BlockBatchRequestData(
        is_complete=True,
        data_paths=dataset_paths,
    )


def is_dataset_ready(
    root_path: str,
    dataset_df: pl.DataFrame,
    dateval: date,
    is_activation_date: bool,
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
        expected = surrounding_dates(dateval, minus_delta=0 if is_activation_date else 1)
        is_ready = sorted(dates_covered) == expected

        if not is_ready:
            missing = [_.isoformat() for _ in sorted(set(expected) - dates_covered)]
            log.warning("missing date coverage", missing=missing)

        return is_ready
