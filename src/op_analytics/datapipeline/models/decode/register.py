from typing import Any

from duckdb.typing import DuckDBPyType
from duckdb.functional import PythonUDFType

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

REGISTERED_FUNCTIONS = []


def register_decoder(
    ctx: DuckDBContext,
    duckdb_function_name: str,
    decoder: Any,
    parameters: list[str],
    return_type: str,
):
    """Register a DuckDB function to decode data."""

    if duckdb_function_name in REGISTERED_FUNCTIONS:
        log.warning(f"duckdb function is already registered: {duckdb_function_name}")
        return

    def _decode(data: str):
        return decoder.decode(data)

    # NOTE: For return types, keep in mind DuckDB does not support converting from python to UHUGEINT
    # https://github.com/duckdb/duckdb/blob/8e68a3e34aa526a342ae91e1b14b764bb3075a12/tools/pythonpkg/src/native/python_conversion.cpp#L325
    ctx.client.create_function(
        duckdb_function_name,
        _decode,
        type=PythonUDFType.NATIVE,
        parameters=[DuckDBPyType(_) for _ in parameters],
        return_type=DuckDBPyType(return_type),
    )

    REGISTERED_FUNCTIONS.append(duckdb_function_name)
