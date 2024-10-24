from datetime import timedelta
import polars as pl

from op_datasets.etl.ingestion.utilities import marker_paths_for_dates, RawOnchainDataLocation
from op_datasets.utils.daterange import DateRange

from .registry import load_model_definitions
from .task import IntermediateModelsTask
from .udfs import create_duckdb_macros


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    read_from: RawOnchainDataLocation,
    write_to: list[RawOnchainDataLocation],
) -> list[IntermediateModelsTask]:
    """Construct a collection of tasks to compute intermediate models.

    While constructing tasks we also go ahead and load the model definitions and create the
    shared duckdb macros that are used across models.
    """

    # Load python functions that define registered data models.
    load_model_definitions()

    # Load shared DuckDB UDFs.
    create_duckdb_macros()

    date_range = DateRange.from_spec(range_spec)

    tasks = []

    # Make one query for all dates and chains.
    #
    # Extend the range of dates +/-1 day to be able to check if there is data
    # continuity on boths ends, which allows us to confirm that the data is ready
    # to be processed.
    query_range = [
        date_range.min - timedelta(days=1),
        date_range.max + timedelta(days=1),
    ] + date_range.dates

    paths_by_dataset_df = marker_paths_for_dates(read_from, query_range, chains)

    for dateval in date_range.dates:
        for chain in chains:
            filtered_df = paths_by_dataset_df.filter(pl.col("chain") == chain)

            tasks.append(
                IntermediateModelsTask.new(
                    dateval=dateval,
                    chain=chain,
                    read_from=read_from,
                    paths_by_dataset_df=filtered_df,
                    models=models,
                    write_to=write_to,
                )
            )

    return tasks
