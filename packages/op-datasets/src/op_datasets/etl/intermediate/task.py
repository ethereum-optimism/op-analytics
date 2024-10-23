from dataclasses import dataclass
from datetime import date
from typing import NewType
import duckdb

import polars as pl
from op_coreutils.duckdb_inmem import parquet_relation


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
