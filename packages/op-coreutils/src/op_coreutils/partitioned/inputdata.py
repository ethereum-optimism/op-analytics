from dataclasses import dataclass
from datetime import date

import polars as pl
from op_datasets.utils.daterange import DateRange

from op_coreutils.logger import bind_contextvars, structlog
from op_coreutils.time import surrounding_dates

from .location import DataLocation
from .marker import markers_for_dates

log = structlog.get_logger()


@dataclass
class InputData:
    # Date
    dateval: date

    # Chain
    chain: str

    # Source
    read_from: DataLocation

    # Input data as parquet paths for each dataset.
    dataset_paths: dict[str, list[str]]
    inputs_ready: bool

    @property
    def contextvars(self):
        return dict(
            chain=self.chain,
            date=self.dateval,
        )


def construct_inputs(
    chains: list[str],
    range_spec: str,
    read_from: DataLocation,
    input_datasets: list[str],
) -> list[InputData]:
    date_range = DateRange.from_spec(range_spec)

    # Make one query for all dates and chains.
    #
    # We use the +/- 1 day padded dates so that we can use the query results to
    # check if there is data on boths ends. This allows us to confirm that the
    # data is ready to be processed.
    markers_df = markers_for_dates(read_from, date_range.padded_dates(), chains)

    inputs = []
    for dateval in date_range.dates:
        for chain in chains:
            bind_contextvars(chain=chain, date=dateval.isoformat())
            filtered_df = markers_df.filter(
                pl.col("chain") == chain,
                pl.col("dt").is_in(surrounding_dates(dateval)),
            )

            # IMPORTANT: At this point the filtered_df contains data for more
            # dates than pertain to this task. This is so we can check data
            # continuity on the day before and after and determine if the input
            # is safe to consume.
            inputs_ready, dataset_paths = are_inputs_ready(
                markers_df=filtered_df,
                dateval=dateval,
                input_datasets=set(input_datasets),
                storage_location=read_from,
            )

            obj = InputData(
                dateval=dateval,
                chain=chain,
                read_from=read_from,
                dataset_paths=dataset_paths or {},
                inputs_ready=inputs_ready,
            )

            inputs.append(obj)

    return inputs


def are_inputs_ready(
    markers_df: pl.DataFrame,
    dateval: date,
    input_datasets: set[str],
    storage_location: DataLocation,
) -> tuple[bool, dict[str, list[str]]]:
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
    all_ready = True

    dataset_paths = {}
    for dataset in input_datasets:
        dataset_df = markers_df.filter(pl.col("dataset_name") == dataset)

        dataset_ready = is_dataset_ready(
            dataset_name=dataset,
            dataset_df=dataset_df,
            dateval=dateval,
        )
        if not dataset_ready:
            all_ready = False

        parquet_paths = []
        for row in dataset_df.filter(pl.col("dt") == dateval).to_dicts():
            parquet_paths.append(storage_location.absolute(row["data_path"]))

        dataset_paths[dataset] = sorted(set(parquet_paths))

    return all_ready, dataset_paths


def is_dataset_ready(dataset_name: str, dataset_df: pl.DataFrame, dateval: date) -> bool:
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
                f"Detected a gap in block numbers: jumps from:#{running_block} to #{next_block}, gap={next_block - running_block}"
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
        log.warning(f"Input data is not complete for {dataset_name!r}. Missing {missing}")

    return is_ready
