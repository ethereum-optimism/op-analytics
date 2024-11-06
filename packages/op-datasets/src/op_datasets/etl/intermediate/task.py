from dataclasses import dataclass
from typing import NewType

import duckdb
from op_coreutils.partitioned import DataReader, DataWriter

BatchDate = NewType("BatchDate", str)


@dataclass(kw_only=True)
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # DataReader
    data_reader: DataReader

    # Models to compute
    models: list[str]

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # DataWriter
    data_writer: DataWriter

    def __repr__(self):
        return (
            self.__class__.__name__
            + f"[ctx: {self.data_reader.contextvars}, datset_paths: {self.data_reader.paths_summary}]"
        )

    def store_output(self, model_name, output_name: str, rel: duckdb.DuckDBPyRelation):
        """Register output data.

        The provided duckdb relation is stored in the task. All outputs that are stored
        in the task will be written out by the DataWriter at the end of execution.
        """
        dataset_name = f"{model_name}/{output_name}"

        if dataset_name in self.output_duckdb_relations:
            raise ValueError(f"name already exists in task outputs: {dataset_name}")

        self.output_duckdb_relations[dataset_name] = rel
