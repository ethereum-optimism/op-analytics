"""DuckDB UDFs that are shared across models."""

import threading
import warnings

import duckdb
import numba
from duckdb.functional import PythonUDFType, FunctionNullHandling
from duckdb.typing import BLOB, INTEGER, VARCHAR, UBIGINT

from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext


@numba.njit
def count_zero_bytes(x: bytes) -> int:
    count = 0
    for ch in x:
        if ch == 0:
            count += 1
    return count


def hex_to_lossy(x: str | None) -> int | None:
    """Assumes that "x" is a hex string with the leading "0x" prefix."""
    if x is None:
        return None

    assert len(x) == 66, f"Expected 66 characters, got {len(x)}: {x}"

    # If the string beyond the 16 right-most bytes is zeros then the conversion
    # to BIGINT will be valid.
    #
    # NOTE (pedrod): I also attempted to use the HUGEINT return type but it resulted
    # in an incorrect conversion from the python type to the duckdb type.
    if x[:-16] == "0x000000000000000000000000000000000000000000000000":
        return int("0x" + x[-16:], 0)

    # There are non-zero bytes beyond the right-most 32 bytes.
    # This means this number cannot be represented as a hugeint.
    return None


def hex_to_lossless(x: str) -> str:
    """Assumes that "x" is a hex string with the leading "0x" prefix."""

    return str(int(x, 0))


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

            duckdb_context.client.create_function(
                "hex_to_lossy",
                hex_to_lossy,
                type=PythonUDFType.NATIVE,
                null_handling=FunctionNullHandling.SPECIAL,
                parameters=[VARCHAR],
                return_type=UBIGINT,
            )

            duckdb_context.client.create_function(
                "hex_to_lossless",
                hex_to_lossless,
                type=PythonUDFType.NATIVE,
                parameters=[VARCHAR],
                return_type=VARCHAR,
            )

        duckdb_context.python_udfs_ready = True


UDFS = [
    """CREATE OR REPLACE MACRO wei_to_eth(a)
    AS a::DECIMAL(28, 0) * 0.000000000000000001::DECIMAL(19, 19);
    """,
    #
    """CREATE OR REPLACE MACRO wei_to_gwei(a)
    AS a::DECIMAL(28, 0) * 0.000000001::DECIMAL(10, 10);
    """,
    #
    """CREATE OR REPLACE MACRO gwei_to_eth(a)
    AS a::DECIMAL(28, 10) * 0.000000001::DECIMAL(10, 10);
    """,
    #
    """CREATE OR REPLACE MACRO safe_div(a, b) AS
    IF(b = 0, NULL, a / b);
    """,
    #
    # Convert a decimal string to float assuming 18 decimal places
    # Split the 18 decimals, add the floating point '.', and then convert to double.
    """CREATE OR REPLACE MACRO decimal_to_float_scale_18(a) AS
    CAST(a[:-19] || '.' || lpad(a[-18:], 18, '0')  AS DOUBLE);
    """,
    #
    # Fee scalars required division by 1e6.
    # The micro function makes the division convenient without losing precision.
    """CREATE OR REPLACE MACRO micro(a)
    AS a * 0.000001::DECIMAL(7, 7);
    """,
    #
    # Truncate a timestamp to hour.
    """CREATE OR REPLACE MACRO epoch_to_hour(a) AS
    date_trunc('hour', make_timestamp(a * 1000000::BIGINT));
    """,
    #
    # Truncate a timestamp to day.
    """CREATE OR REPLACE MACRO epoch_to_day(a) AS
    date_trunc('day', make_timestamp(a * 1000000::BIGINT));
    """,
    #
    # Division by 16 for DECIMAL types.
    """CREATE OR REPLACE MACRO div16(a)
    AS a * 0.0625::DECIMAL(5, 5);
    """,
    #
    # Count zero bytes for binary data that is encoded as a hex string.
    """CREATE OR REPLACE MACRO hexstr_zero_bytes(a)
    AS count_zero_bytes(unhex(substr(a, 3)));
    """,
    #
    # Get the length in bytes for binary data that is encoded as a hex string.
    """CREATE OR REPLACE MACRO hexstr_bytelen(x)
    AS CAST((length(x) - 2) / 2 AS INT);
    """
    #
    # Calculate calldata gas used for binary data that is encoded as a hex
    # string (can be updated by an EIP).
    """CREATE OR REPLACE MACRO hexstr_calldata_gas(x)
    AS 16 * (hexstr_bytelen(x) - hexstr_zero_bytes(x)) + 4 * hexstr_zero_bytes(x);
    """,
    #
    # Get the method id for input data. This is the first 4 bytes, or first 10
    # string characters for binary data that is encoded as a hex string.
    """CREATE OR REPLACE MACRO hexstr_method_id(x)
    AS substring(x,1,10);
    """,
    #
    # Trace address depth. Examples:
    #  ""       -> 0
    #  "0"      -> 1
    #  "0,2"    -> 2
    #  "0,10,0" -> 3
    """CREATE OR REPLACE MACRO trace_address_depth(a)
    AS CASE
      WHEN length(a) = 0 THEN 0
      ELSE length(a) - length(replace(a, ',', '')) + 1
    END;
    """,
    #
    # Trace address parent. Examples:
    #  ""       -> "none"
    #  "0"      -> ""
    #  "0,2"    -> "0"
    #  "0,10,0" -> "0,10"
    #  "0,100,0" -> "0,100"
    """CREATE OR REPLACE MACRO trace_address_parent(a)
    AS CASE
      WHEN length(a) = 0 THEN 'none'
      ELSE substring(a, 1, length(a) - 1 - length(split_part(a, ',', -1)))
    END;
    """,
    #
    # Trace address root. Examples:
    #  ""       -> -1
    #  "0"      -> 0
    #  "0,2"    -> 0
    #  "0,10,0" -> 0
    #  "0,100,0" -> 0
    """CREATE OR REPLACE MACRO trace_address_root(a)
    AS CASE
      WHEN length(a) = 0 THEN -1
      ELSE CAST(split_part(a, ',', 1) AS INTEGER)
    END;
    """,
    #
    # Convert indexed event arg to address.
    """CREATE OR REPLACE MACRO indexed_event_arg_to_address(a)
    AS concat('0x', right(a, 40));
    """,
]


def create_duckdb_macros(duckdb_context: DuckDBContext):
    """Create general purpose macros on the DuckDB in-memory client.

    These macros can be used as part of data model definitions.
    """

    create_python_udfs(duckdb_context)

    duckdb_context.client.sql("\n ".join(UDFS))


def set_memory_limit(duckdb_client: duckdb.DuckDBPyConnection, gb: int):
    duckdb_client.sql(f"SET memory_limit = '{gb}GB'")
