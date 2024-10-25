import polars as pl
from op_coreutils.time import surrounding_dates

from op_datasets.etl.ingestion.utilities import RawOnchainDataLocation, markers_for_dates
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
    # We use the +/- 1 day padded dates so that we can use the query results to
    # check if there is data on boths ends. This allows us to confirm that the
    # data is ready to be processed.
    markers_df = markers_for_dates(read_from, date_range.padded_dates(), chains)

    for dateval in date_range.dates:
        for chain in chains:
            filtered = markers_df.filter(
                pl.col("chain") == chain,
                pl.col("dt").is_in(surrounding_dates(dateval)),
            )

            tasks.append(
                IntermediateModelsTask.new(
                    dateval=dateval,
                    chain=chain,
                    read_from=read_from,
                    markers_df=filtered,
                    models=models,
                    write_to=write_to,
                )
            )

    return tasks
