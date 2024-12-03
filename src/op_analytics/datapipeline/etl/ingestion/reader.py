from datetime import date

import polars as pl

from op_analytics.coreutils.logger import bound_contextvars, structlog
from op_analytics.coreutils.partitioned.dataaccess import init_data_access
from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.coreutils.partitioned.output import PartitionColumns
from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.time import date_fromstr, surrounding_dates
from op_analytics.datapipeline.utils.daterange import DateRange

log = structlog.get_logger()


def construct_readers(
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
            if not is_chain_active(chain, dateval):
                log.info(
                    f"skipping inactive chain: {str(dateval)} {chain} ",
                )
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
                inputs_ready, dataset_paths = are_inputs_ready(
                    markers_df=filtered_df,
                    dateval=dateval,
                    input_datasets=set(dataset_names),
                    storage_location=read_from,
                )

                obj = DataReader(
                    partitions=PartitionColumns.from_tuples(
                        [
                            ("chain", chain),
                            ("dt", dateval.strftime("%Y-%m-%d")),
                        ]
                    ),
                    read_from=read_from,
                    dataset_paths=dataset_paths or {},
                    inputs_ready=inputs_ready,
                )

                inputs.append(obj)
                if not inputs_ready:
                    log.warning("MISSING DATA")
                    num_suspect += 1

    log.info(f"prepared {len(inputs)} input batches.")
    if num_suspect > 0:
        log.info(f"input not ready for {num_suspect} batches.")

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


CHAIN_ACTIVATION_DATES = {
    "automata": date_fromstr("2024-07-17"),
    "base": date_fromstr("2023-06-15"),
    "bob": date_fromstr("2024-04-11"),
    "cyber": date_fromstr("2024-04-18"),
    "fraxtal": date_fromstr("2024-02-01"),
    "ham": date_fromstr("2024-05-24"),
    "kroma": date_fromstr("2023-09-05"),
    "lisk": date_fromstr("2024-05-03"),
    "lyra": date_fromstr("2023-11-15"),
    "metal": date_fromstr("2024-03-27"),
    "mint": date_fromstr("2024-05-13"),
    "mode": date_fromstr("2023-11-16"),
    "op": date_fromstr("2021-11-12"),
    "orderly": date_fromstr("2023-10-06"),
    "polynomial": date_fromstr("2024-06-10"),
    "race": date_fromstr("2024-07-08"),
    "redstone": date_fromstr("2024-04-03"),
    "shape": date_fromstr("2024-07-23"),
    "swan": date_fromstr("2024-06-18"),
    "unichain": date_fromstr("2024-11-04"),
    "worldchain": date_fromstr("2024-06-25"),
    "xterio": date_fromstr("2024-05-24"),
    "zora": date_fromstr("2023-06-13"),
    # TESTNETS
    "op_sepolia": date_fromstr("2024-01-01"),
    "unichain_sepolia": date_fromstr("2024-09-19"),
}


def is_chain_active(chain: str, dateval: date) -> bool:
    activation = CHAIN_ACTIVATION_DATES[chain]

    return dateval >= activation
