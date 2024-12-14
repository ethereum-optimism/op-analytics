"""DuckDB UDFs that are shared across intermediate models."""

import duckdb


def create_duckdb_macros(duckdb_client: duckdb.DuckDBPyConnection):
    """Create general purpose macros on the DuckDB in-memory client.

    These macros can be used as part of data model definitions.
    """

    duckdb_client.sql("""
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
    
    -- Split a hex string into an array of individual bytes.
    -- Example: 0x3d602d80000a --> [3d, 60, 2d, 80, 00, 0a]
    CREATE OR REPLACE MACRO hexstr_array(a)
    AS generate_series(1, (a).substr(3).length(), 2).list_transform(x -> (a).substr(3).substr(x, 2));
                      
    -- Count non-zero bytes for binary data that is encoded as a hex string.
    -- We don't use hexstr_bytelen because we need to substring the input data.
    CREATE OR REPLACE MACRO hexstr_nonzero_bytes(a)
    AS hexstr_array(a).list_filter(x -> x != '00').length();
    
    -- Count zero bytes for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_zero_bytes(a)
    AS hexstr_array(a).list_filter(x -> x == '00').length();
    
    -- Calculate calldata gas used for binary data that is encoded as a hex
    -- string (can be updated by an EIP).
    CREATE OR REPLACE MACRO hexstr_calldata_gas(x)
    AS 16 * hexstr_nonzero_bytes(x) + 4 * hexstr_zero_bytes(x);
    
    --Get the method id for input data. This is the first 4 bytes, or first 10
    -- string characters for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_method_id(x)
    AS substring(x,1,10)
    """)


def set_memory_limit(duckdb_client: duckdb.DuckDBPyConnection, gb: int):
    duckdb_client.sql(f"SET memory_limit = '{gb}GB'")
