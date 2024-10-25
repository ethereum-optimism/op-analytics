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
    input_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]
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

    @property
    def dt(self):
        return self.dateval.strftime("%Y-%m-%d")

    @property
    def contextvars(self):
        return dict(
            chain=self.chain,
            date=self.dt,
        )

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

        duckb_parquet_relations = {}
        for name, paths in (dataset_paths or {}).items():
            duckb_parquet_relations[name] = parquet_relation(paths)

        new_obj = cls(
            dateval=dateval,
            chain=chain,
            read_from=read_from,
            input_duckdb_relations=duckb_parquet_relations,
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
