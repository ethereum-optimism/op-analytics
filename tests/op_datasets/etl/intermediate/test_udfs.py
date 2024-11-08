from decimal import Decimal

from op_coreutils.duckdb_inmem import init_client
from op_datasets.etl.intermediate.udfs import (
    create_duckdb_macros,
    Expression,
    safe_div,
    wei_to_eth,
    wei_to_gwei,
)


def test_macros():
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
        Expression(alias="ans_eth", sql_expr=wei_to_eth("gas_price * receipt_gas_used")),
        Expression(alias="ans_gwei", sql_expr=wei_to_gwei("gas_price * receipt_gas_used")),
        Expression(alias="ans_division_ok", sql_expr=safe_div("receipt_gas_used", "fifty")),
        Expression(alias="ans_division_err", sql_expr=safe_div("receipt_gas_used", "zero")),
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
