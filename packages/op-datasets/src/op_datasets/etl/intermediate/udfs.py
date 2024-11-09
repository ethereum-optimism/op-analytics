"""DuckDB UDFs that are shared across intermediate models."""

import duckdb

from dataclasses import dataclass


@dataclass
class Expression:
    """Helper class to hold the definition of an expression along with its alias."""

    alias: str
    sql_expr: str

    @property
    def expr(self):
        return self.sql_expr + " AS " + self.alias


def create_duckdb_macros(duckdb_client: duckdb.DuckDBPyConnection):
    """Create general purpose macros on the DuckDB in-memory client.

    These macros can be used as part of data model definitions.
    """

    duckdb_client.sql("""
    CREATE OR REPLACE MACRO wei_to_eth(a)
    AS a::DECIMAL(28, 0) * 0.000000000000000001::DECIMAL(19, 19);

    CREATE OR REPLACE MACRO wei_to_gwei(a)
    AS a::DECIMAL(28, 0) * 0.000000001::DECIMAL(10, 10);

    CREATE OR REPLACE MACRO safe_div(a, b) AS
    IF(b = 0, NULL, a / b);
    
    -- Fee scalars required division by 1e6.
    -- The micro function makes the division convenient without losing precision.
    CREATE OR REPLACE MACRO micro(a)
    AS a / 1000000;
    
    CREATE OR REPLACE MACRO epoch_to_hour(a) AS 
    date_trunc('hour', make_timestamp(a * 1000000::BIGINT))
    """)


# The functions below are defined for cosmetic purposes. When used they add syntax highlighting
# to SQL expressions which makes them easier to read.


def wei_to_eth(x):
    return f"wei_to_eth({x})"


def wei_to_gwei(x):
    return f"wei_to_gwei({x})"


def safe_div(x, y):
    return f"safe_div({x}, {y})"
