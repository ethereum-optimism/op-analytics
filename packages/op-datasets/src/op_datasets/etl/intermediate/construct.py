from op_datasets.etl.ingestion.utilities import marker_paths_for_dates
from op_datasets.utils.daterange import DateRange

from .registry import load_model_definitions
from .task import IntermediateModelsTask
from .udfs import create_duckdb_macros


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    source_spec: str,
    sinks_spec: list[str],
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

    for dateval in date_range.dates:
        paths_by_dataset_df = marker_paths_for_dates("OPLABS_CLICKHOUSE", [dateval], chains)

        # TODO: Determine if the data is complete and ready to be consumed.

        for chain in chains:
            tasks.append(
                IntermediateModelsTask.new(
                    dateval=dateval,
                    chain=chain,
                    paths_by_dataset_df=paths_by_dataset_df,
                    models=models,
                )
            )

    return tasks
