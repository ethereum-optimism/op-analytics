from datetime import datetime
from decimal import Decimal

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.datapipeline.etl.intermediate.udfs import (
    create_duckdb_macros,
    Expr,
    safe_div,
    wei_to_eth,
    wei_to_gwei,
)


def test_macros_00():
    client = init_client()
    create_duckdb_macros(client)

    client.sql("""
    CREATE TABLE test_macros AS 
    SELECT 
        100::BIGINT AS gas_price,
        200::BIGINT AS receipt_gas_used,
        0 AS zero,
        50 as fifty
    """)

    # Use the Expression class to create the sql.
    exprs = [
        Expr(alias="ans_eth", expr=wei_to_eth("gas_price * receipt_gas_used")),
        Expr(alias="ans_gwei", expr=wei_to_gwei("gas_price * receipt_gas_used")),
        Expr(alias="ans_division_ok", expr=safe_div("receipt_gas_used", "fifty")),
        Expr(alias="ans_division_err", expr=safe_div("receipt_gas_used", "zero")),
    ]

    delimited_exprs = ",\n    ".join([_.expr for _ in exprs])
    result1 = client.sql(f"""
    SELECT gas_price * receipt_gas_used, {delimited_exprs} FROM test_macros
    """)
    actual1 = result1.fetchall()
    expected = [(20000, Decimal("2.00000E-14"), Decimal("0.0000200000"), 4.0, None)]

    assert actual1 == expected

    # Use raw sql.
    result2 = client.sql("""
    SELECT
        gas_price * receipt_gas_used,
        wei_to_eth(gas_price * receipt_gas_used) AS ans_eth,
        wei_to_gwei(gas_price * receipt_gas_used) AS ans_gwei,
        safe_div(receipt_gas_used, fifty) AS ans_division_ok,
        safe_div(receipt_gas_used, zero) AS ans_division_err
    FROM test_macros
    """)
    actual2 = result2.fetchall()
    assert actual2 == expected


def test_epoch_to_hour():
    client = init_client()
    create_duckdb_macros(client)

    actual = client.sql("""
    SELECT 
        100::BIGINT AS timestamp,
        epoch_to_hour(100::INT) as hour1,
        epoch_to_hour(3700::INT) as hour2,
        epoch_to_hour(1731176747) as hour3
     """).fetchall()[0]

    expected = (
        100,
        datetime(1970, 1, 1, 0, 0),
        datetime(1970, 1, 1, 1, 0),
        datetime(2024, 11, 9, 18, 0),
    )
    assert actual == expected


def test_micro():
    client = init_client()
    create_duckdb_macros(client)

    actual = client.sql("""
    SELECT 
        micro(100) AS m1,
        micro(1000000) as m2
     """).fetchall()[0]

    expected = (
        Decimal("0.0001000"),
        Decimal("1.0000000"),
    )
    assert actual == expected


def test_div16():
    client = init_client()
    create_duckdb_macros(client)

    actual = client.sql("""
    SELECT 
        div16(128) AS m1,
        div16(1) as m2
     """).fetchall()[0]

    expected = (
        Decimal("8.00000"),
        Decimal("0.06250"),
    )
    assert actual == expected


def test_hexstr_bytelen():
    client = init_client()
    create_duckdb_macros(client)

    actual = client.sql("""
    SELECT 
        hexstr_bytelen('0x3d602d80600a3d3981f3363d3d373d3d3d363d739ec1c3dcf667f2035fb4cd2eb42a1566fd54d2b75af43d82803e903d91602b57fd5bf3') AS m1,
        hexstr_bytelen('0x3d60')
     """).fetchall()[0]

    expected = (
        55,
        2,
    )
    assert actual == expected
