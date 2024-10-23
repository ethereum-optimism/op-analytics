from datetime import date

import polars as pl
from op_coreutils.clickhouse import run_oplabs_query

from op_datasets.etl.ingestion.sinks import MARKERS_DB, MARKERS_TABLE
from op_datasets.utils.daterange import DateRange

from .registry import load_model_definitions
from .udfs import create_duckdb_macros
from .task import IntermediateModelsTask


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
        paths_by_dataset_df = query_completion_markers(dateval, chains)

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


def query_completion_markers(dateval: date, chains: list[str]) -> pl.DataFrame:
    # Assume source spec is GCS
    select = f"SELECT marker_path, outputs.full_path, chain FROM {MARKERS_DB}.{MARKERS_TABLE}"

    markers = run_oplabs_query(
        query=select + " WHERE dt = {date:Date} AND chain in {chains:Array(String)}",
        parameters={"date": dateval, "chains": chains},
    )

    paths_df = markers.select(
        pl.col("chain"),
        pl.col("outputs.full_path").alias("parquet_path"),
    ).explode("parquet_path")

    paths_by_dataset_df = (
        paths_df.with_columns(
            dataset=pl.col("parquet_path").map_elements(
                _marker_path_to_dataset,
                return_dtype=pl.String,
            )
        )
        .group_by("chain", "dataset")
        .agg(pl.col("parquet_path"))
    )

    assert paths_by_dataset_df.schema == {
        "chain": pl.String,
        "dataset": pl.String,
        "parquet_path": pl.List(pl.String),
    }
    return paths_by_dataset_df


def _marker_path_to_dataset(path: str) -> str:
    if path.startswith("ingestion/blocks"):
        return "blocks"
    if path.startswith("ingestion/transactions"):
        return "transactions"
    if path.startswith("ingestion/logs"):
        return "logs"
    if path.startswith("ingestion/traces"):
        return "traces"

    raise NotImplementedError()
