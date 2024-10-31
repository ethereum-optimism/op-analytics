from dataclasses import dataclass
from datetime import date
from typing import NewType

import duckdb
import polars as pl
from op_coreutils.duckdb_inmem import parquet_relation
from op_coreutils.partitioned import SinkMarkerPath, DataLocation


from .status import are_inputs_ready

BatchDate = NewType("BatchDate", str)


@dataclass(kw_only=True)
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # Date
    dateval: date

    # Chain
    chain: str

    # Source
    read_from: DataLocation

    # Input data as duckdb parquet relations by dataset.
    dataset_paths: dict[str, list[str]]
    inputs_ready: bool

    # Models to compute
    models: list[str]

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    force: bool  # ignores completion markers when set to true

    # Sinks
    write_to: list[DataLocation]

    # # Expected Markers
    expected_markers: list[SinkMarkerPath]
    is_complete: bool

    def __repr__(self):
        return (
            self.__class__.__name__
            + f"[ctx: {self.contextvars}, datset_paths: {self.paths_summary}]"
        )

    @property
    def dt(self):
        return self.dateval.strftime("%Y-%m-%d")

    @property
    def contextvars(self):
        return dict(
            chain=self.chain,
            date=self.dt,
        )

    @property
    def paths_summary(self):
        return {dataset_name: len(paths) for dataset_name, paths in self.dataset_paths.items()}

    def duckdb_relation(self, dataset):
        return parquet_relation(self.dataset_paths[dataset])

    @classmethod
    def new(
        cls,
        dateval: date,
        chain: str,
        markers_df: pl.DataFrame,
        read_from: DataLocation,
        write_to: list[DataLocation],
        models: list[str],
    ):
        # IMPORTANT: At this point the paths_by_dataset_df contains
        # data for more dates than pertain to this task. This is for
        # us to check data continuity so we can determine if the
        # inputs are ready.
        dataset_paths = are_inputs_ready(
            markers_df,
            dateval,
            expected_datasets={
                "blocks",
                "transactions",
                "traces",
                "logs",
            },
            storage_location=read_from,
        )

        new_obj = cls(
            dateval=dateval,
            chain=chain,
            read_from=read_from,
            dataset_paths=dataset_paths or {},
            inputs_ready=dataset_paths is not None,
            models=models,
            output_duckdb_relations={},
            write_to=write_to,
            force=False,
            expected_markers=[],
            is_complete=False,
        )

        # TODO: Compute what are the expected markers.

        return new_obj

    def add_output(self, name: str, output: duckdb.DuckDBPyRelation):
        if name in self.output_duckdb_relations:
            raise ValueError(f"name already exists in task outputs: {name}")
        self.output_duckdb_relations[name] = output
