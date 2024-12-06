from dataclasses import dataclass

import duckdb

from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writer import DataWriter


@dataclass(kw_only=True)
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # Model to compute
    model: str

    # DataReader
    data_reader: DataReader

    # DataWriter
    data_writer: DataWriter

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # Top directory where the results of the model will be stored.
    output_root_path_prefix: str
