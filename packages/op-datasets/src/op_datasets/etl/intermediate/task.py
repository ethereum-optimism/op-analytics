from dataclasses import dataclass
from typing import NewType

import duckdb
from op_coreutils.duckdb_inmem import parquet_relation
from op_coreutils.partitioned import InputData, DataWriter

BatchDate = NewType("BatchDate", str)


@dataclass(kw_only=True)
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # Input data
    inputdata: InputData

    # Models to compute
    models: list[str]

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    force: bool  # ignores completion markers when set to true

    # DataWriter
    data_writer: DataWriter

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
        return self.inputdata.contextvars

    @property
    def paths_summary(self):
        return {dataset_name: len(paths) for dataset_name, paths in self.dataset_paths.items()}

    def duckdb_relation(self, dataset):
        return parquet_relation(self.dataset_paths[dataset])

    def add_output(self, name: str, output: duckdb.DuckDBPyRelation):
        if name in self.output_duckdb_relations:
            raise ValueError(f"name already exists in task outputs: {name}")
        self.output_duckdb_relations[name] = output
