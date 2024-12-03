from dataclasses import dataclass
from typing import NewType

import duckdb

from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import DataWriter

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
            + f"[ctx: {self.data_reader.partitions_dict()}, datset_paths: {self.data_reader.paths_summary}]"
        )
