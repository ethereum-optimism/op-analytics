from dataclasses import dataclass
from datetime import date
from typing import NewType
import duckdb

import polars as pl
from op_coreutils.duckdb_inmem import parquet_relation
from op_coreutils.clickhouse import run_oplabs_query

from op_datasets.etl.ingestion.sinks import MARKERS_DB, MARKERS_TABLE
from op_datasets.utils.daterange import DateRange

BatchDate = NewType("BatchDate", str)


@dataclass
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # Date
    dt: BatchDate

    # Chain
    chain: str

    # Input data as duckdb parquet relations by dataset.
    input_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # Models to compute
    models: list[str]

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # # Inputs
    # input_datasets: dict[str, CoreDataset]
    # input_dataframes: dict[str, pl.DataFrame]

    # # Outputs
    # output_dataframes: list[OutputDataFrame]
    # force: bool  # ignores completion markers when set to true

    # # Sinks
    # sinks: list[DataSink]

    # # Expected Markers
    # expected_markers: list[SinkMarkerPath]
    # is_complete: bool

    @classmethod
    def new(cls, dateval: date, chain: str, paths_by_dataset_df: pl.DataFrame, models: list[str]):
        paths_by_dataset_map = {}
        for row in paths_by_dataset_df.filter(pl.col("chain") == chain).to_dicts():
            dataset_name = row["dataset"]
            parquet_paths = sorted(set(row["parquet_path"]))
            paths_by_dataset_map[dataset_name] = parquet_paths

        duckb_parquet_relations = {
            name: parquet_relation(paths) for name, paths in paths_by_dataset_map.items()
        }

        return cls(
            dt=BatchDate(dateval.strftime("%Y-%m-%d")),
            chain=chain,
            input_duckdb_relations=duckb_parquet_relations,
            models=models,
            output_duckdb_relations={},
        )

    def add_output(self, name: str, output: duckdb.DuckDBPyRelation):
        if name in self.output_duckdb_relations:
            raise ValueError(f"name already exists in task outputs: {name}")
        self.output_duckdb_relations[name] = output


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


def construct_tasks(
    chains: list[str],
    models: list[str],
    range_spec: str,
    source_spec: str,
    sinks_spec: list[str],
) -> list[IntermediateModelsTask]:
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
