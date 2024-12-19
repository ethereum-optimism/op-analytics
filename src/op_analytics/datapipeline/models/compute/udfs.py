"""DuckDB UDFs that are shared across intermediate models."""

import threading
import warnings

import duckdb
import numba
from duckdb.functional import PythonUDFType
from duckdb.typing import BLOB, INTEGER

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext


@numba.njit
def count_zero_bytes(x: bytes) -> int:
    count = 0
    for ch in x:
        if ch == 0:
            count += 1
    return count


_UDF_LOCK = threading.Lock()


def create_python_udfs(duckdb_context: DuckDBContext):
    """Decorated with @cache so it only runs once."""

    with _UDF_LOCK:
        if duckdb_context.python_udfs_ready:
            return

        with warnings.catch_warnings():
            # Suppressing the following warning from duckdb:
            # DeprecationWarning: numpy.core is deprecated and has been renamed to numpy._core.
            warnings.simplefilter("ignore", DeprecationWarning)

            duckdb_context.client.create_function(
                "count_zero_bytes",
                count_zero_bytes,
                type=PythonUDFType.NATIVE,
                parameters=[BLOB],
                return_type=INTEGER,
            )
        duckdb_context.python_udfs_ready = True


def create_duckdb_macros(duckdb_context: DuckDBContext):
    """Create general purpose macros on the DuckDB in-memory client.

    These macros can be used as part of data model definitions.
    """

    create_python_udfs(duckdb_context)

    duckdb_context.client.sql("""
    CREATE OR REPLACE MACRO wei_to_eth(a)
    AS a::DECIMAL(28, 0) * 0.000000000000000001::DECIMAL(19, 19);

    CREATE OR REPLACE MACRO wei_to_gwei(a)
    AS a::DECIMAL(28, 0) * 0.000000001::DECIMAL(10, 10);
    
    CREATE OR REPLACE MACRO gwei_to_eth(a)
    AS a::DECIMAL(28, 10) * 0.000000001::DECIMAL(10, 10);

    CREATE OR REPLACE MACRO safe_div(a, b) AS
    IF(b = 0, NULL, a / b);

    -- Fee scalars required division by 1e6.
    -- The micro function makes the division convenient without losing precision.
    CREATE OR REPLACE MACRO micro(a)
    AS a * 0.000001::DECIMAL(7, 7);

    -- Truncate a timestamp to hour.
    CREATE OR REPLACE MACRO epoch_to_hour(a) AS
    date_trunc('hour', make_timestamp(a * 1000000::BIGINT));
    
    -- Truncate a timestamp to day.
    CREATE OR REPLACE MACRO epoch_to_day(a) AS
    date_trunc('day', make_timestamp(a * 1000000::BIGINT));

    -- Division by 16 for DECIMAL types.
    CREATE OR REPLACE MACRO div16(a)
    AS a * 0.0625::DECIMAL(5, 5);
    
    -- Get the length in bytes for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_bytelen(x)
    AS CAST((length(x) - 2) / 2 AS INT);
    
    -- Count zero bytes for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_zero_bytes(a)
    AS count_zero_bytes(unhex(substr(a, 3)));
    
    -- Calculate calldata gas used for binary data that is encoded as a hex
    -- string (can be updated by an EIP).
    CREATE OR REPLACE MACRO hexstr_calldata_gas(x)
    AS 16 * (hexstr_bytelen(x) - hexstr_zero_bytes(x)) + 4 * hexstr_zero_bytes(x);
    
    --Get the method id for input data. This is the first 4 bytes, or first 10
    -- string characters for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_method_id(x)
    AS substring(x,1,10);
    
    -- Trace address depth. Examples:
    --   ""       -> 0
    --   "0"      -> 1
    --   "0,2"    -> 2
    --   "0,10,0" -> 3
    CREATE OR REPLACE MACRO trace_address_depth(a)
    AS CASE
      WHEN length(a) = 0 THEN 0
      WHEN length(a) = 1 THEN 1
      ELSE (length(a) + 1) // 2
    END;

    -- Trace address parent. Examples:
    --   ""       -> root
    --   "0"      -> first
    --   "0,2"    -> 0
    --   "0,10,0" -> 0,10
    CREATE OR REPLACE MACRO trace_address_parent(a)
    AS CASE
      WHEN length(a) = 0 THEN 'root'
      WHEN length(a) = 1 THEN 'first'
      ELSE a[:-3]
    END;
    """)


def set_memory_limit(duckdb_client: duckdb.DuckDBPyConnection, gb: int):
    duckdb_client.sql(f"SET memory_limit = '{gb}GB'")
