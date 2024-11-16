from dataclasses import dataclass
from datetime import date

import duckdb
import polars as pl
from op_analytics.datapipeline.utils.daterange import DateRange

from op_analytics.coreutils.duckdb_inmem import parquet_relation
from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.time import surrounding_dates

from .location import DataLocation
from .dataaccess import init_data_access

log = structlog.get_logger()


@dataclass
class DataReader:
    """Manages reading partitioned data.

    When objects of this class are created the creator must be aware of whether
    the data is ready to be consumed.

    A specfic task may decide what to do with the current parquet paths in GCS
    on cases when the data is not yet ready to be consumed.

    On some cases it will be preferable to do no processing for incomplete data
    (e.g. when processing intermediate models).

    On other cases it can make sense to do partial processing (e.g. when loading
    data into BigQuery).
    """

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
            date=self.dateval.strftime("%Y-%m-%d"),
        )

    @property
    def paths_summary(self) -> dict[str, int]:
        return {dataset_name: len(paths) for dataset_name, paths in self.dataset_paths.items()}

    def duckdb_relation(self, dataset) -> duckdb.DuckDBPyRelation:
        return parquet_relation(self.dataset_paths[dataset])


def construct_input_batches(
    chains: list[str],
    range_spec: str,
    read_from: DataLocation,
    markers_table: str,
    dataset_names: list[str],
) -> list[DataReader]:
    """Construct a list of DataReader for the given parameters.

    The parameters specify a set of chains, dates, and datasets that we are
    interested in processing.

    The DataReader will have knowledge of the parquet uris that comprise the
    input data. It can be used to load the data onto BigQuery or to run an
    intermediate model over the date.
    """
    client = init_data_access()

    date_range = DateRange.from_spec(range_spec)

    # Make one query for all dates and chains.
    #
    # We use the +/- 1 day padded dates so that we can use the query results to
    # check if there is data on boths ends. This allows us to confirm that the
    # data is ready to be processed.
    markers_df = client.markers_for_raw_ingestion(
        data_location=read_from,
        markers_table=markers_table,
        datevals=date_range.padded_dates(),
        chains=chains,
        dataset_names=dataset_names,
    )

    num_suspect = 0
    inputs = []
    for dateval in date_range.dates:
        for chain in chains:
            with bound_contextvars(chain=chain, date=dateval.isoformat()):
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
                    input_datasets=set(dataset_names),
                    storage_location=read_from,
                )

                obj = DataReader(
                    dateval=dateval,
                    chain=chain,
                    read_from=read_from,
                    dataset_paths=dataset_paths or {},
                    inputs_ready=inputs_ready,
                )

                inputs.append(obj)
                if not inputs_ready:
                    log.warning("MISSING DATA")
                    num_suspect += 1

    log.info(
        f"prepared {len(inputs)} input batches. {num_suspect} batches where input was not ready but will be ingested anyways."
    )
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

    assert dict(markers_df.schema) == {
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
    with bound_contextvars(dataset=dataset_name):
        if dataset_df.is_empty():
            log.warning("no data")
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
