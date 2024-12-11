from dataclasses import dataclass

import duckdb

from op_analytics.coreutils.partitioned.reader import DataReader
from op_analytics.coreutils.partitioned.writehelper import WriteManager
from op_analytics.datapipeline.models.compute.modelexecute import PythonModel


@dataclass(kw_only=True)
class IntermediateModelsTask:
    """All info and data required to process intermediate models for a date.

    This object is mutated during processing.
    """

    # Model to compute
    model: PythonModel

    # DataReader
    data_reader: DataReader

    # Write Manager
    write_manager: WriteManager

    # Output duckdb relations
    output_duckdb_relations: dict[str, duckdb.DuckDBPyRelation]

    # Top directory where the results of the model will be stored.
    output_root_path_prefix: str
