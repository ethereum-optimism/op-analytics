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
    AS wei_to_gwei(a);

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
    
    --Get the length in bytes for binary data that is encoded as a hex string
    CREATE OR REPLACE MACRO hexstr_bytelen(x)
    AS (length(x) - 2) / 2;
                      
    --Count non-zero bytes for binary data that is encoded as a hex string. We don't use hexstr_bytelen because we need to substring the input data.
    CREATE OR REPLACE MACRO hexstr_nonzero_bytes(x)
    AS length( REPLACE(TO_HEX(FROM_HEX(SUBSTR(x, 3))), '00', '') ) / 2;
    
    --Count non-zero bytes for binary data that is encoded as a hex string
    CREATE OR REPLACE MACRO hexstr_zero_bytes(x)
    AS hexstr_bytelen(x) - hexstr_nonzero_bytes(x);
    
    --Calculate calldata gas used for binary data that is encoded as a hex string (can be updated by an EIP)
    CREATE OR REPLACE MACRO hexstr_calldata_gas(x)
    AS 16*hexstr_nonzero_bytes(x) + 4*hexstr_zero_bytes(x);
    
    --Get the method id for input data. This is the first 4 bytes, or first 10 string characters for binary data that is encoded as a hex string.
    CREATE OR REPLACE MACRO hexstr_method_id(x)
    AS substring(x,1,10);
    
    -- TODO: Is there some way that we could or should pass in the alias for each table?
    -- Calculate total gas fee in native units, given transactions t and blocks b
    CREATE OR REPLACE MACRO total_gas_fee_native()
    AS CASE WHEN t.gas_price = 0 THEN 0 ELSE wei_to_eth(t.receipt_l1_fee + t.gas_price * t.receipt_gas_used) END;
    
    -- Calculate l2 base fee gas fee in native units, given transactions t and blocks b
    CREATE OR REPLACE MACRO l2_base_gas_fee_native()
    AS CASE WHEN t.gas_price = 0 THEN 0 ELSE wei_to_eth(t.receipt_l1_fee + t.gas_price * t.receipt_gas_used) END;
    
    -- Calculate l2 priority fee gas fee in native units, given transactions t and blocks b
    CREATE OR REPLACE MACRO l2_priority_gas_fee_native()
    AS CASE WHEN t.gas_price = 0 THEN 0 ELSE wei_to_eth(t.receipt_l1_fee + t.gas_price * t.receipt_gas_used) END;

    -- Calculate l1 data fee gas fee in native units, given transactions t and blocks b
    CREATE OR REPLACE MACRO l2_gas_fee_native()                 
    AS wei_to_eth(t.gas_price * t.receipt_gas_used
    
    -- Calculate l1 data fee gas fee in native units, given transactions t and blocks b
    CREATE OR REPLACE MACRO l1_gas_fee_native()                 
    AS CASE WHEN t.gas_price = 0 THEN 0 ELSE wei_to_eth(t.receipt_l1_fee) END
    """)


def set_memory_limit(duckdb_client: duckdb.DuckDBPyConnection, gb: int):
    duckdb_client.sql(f"SET memory_limit = '{gb}GB'")
