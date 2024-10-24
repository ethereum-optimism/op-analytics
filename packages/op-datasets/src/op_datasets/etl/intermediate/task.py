from dataclasses import dataclass
from datetime import date
from typing import NewType
import duckdb

import polars as pl
from op_coreutils.duckdb_inmem import parquet_relation
from op_datasets.etl.ingestion.utilities import RawOnchainDataLocation
from op_coreutils.storage.paths import SinkMarkerPath

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
    read_from: RawOnchainDataLocation

    # Input data as duckdb parquet relations by dataset.
    input_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]
    inputs_ready: bool

    # Models to compute
    models: list[str]

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    force: bool  # ignores completion markers when set to true

    # Sinks
    write_to: list[RawOnchainDataLocation]

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
        paths_by_dataset_df: pl.DataFrame,
        read_from: RawOnchainDataLocation,
        write_to: list[RawOnchainDataLocation],
        models: list[str],
    ):
        # IMPORTANT: At this point the paths_by_dataset_df contains
        # data for more days than pertain to this task. This is for
        # us to check data continuity so we can determine if the
        # inputs are ready.

        inputs_ready = are_inputs_ready(paths_by_dataset_df, dateval)

        paths_by_dataset_map = {}
        for row in paths_by_dataset_df.filter(pl.col("dt") == dateval).to_dicts():
            dataset_name = row["dataset"]
            parquet_paths = sorted(set(row["parquet_path"]))
            paths_by_dataset_map[dataset_name] = parquet_paths

        duckb_parquet_relations = {
            name: parquet_relation(paths) for name, paths in paths_by_dataset_map.items()
        }

        new_obj = cls(
            dateval=dateval,
            chain=chain,
            read_from=read_from,
            input_duckdb_relations=duckb_parquet_relations,
            inputs_ready=inputs_ready,
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
