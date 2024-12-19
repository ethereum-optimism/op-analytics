import datetime
from decimal import Decimal

from op_analytics.coreutils.duckdb_inmem import init_client
from op_analytics.datapipeline.models.compute.udfs import (
    create_duckdb_macros,
)


def test_macros_00():
    ctx = init_client()
    create_duckdb_macros(ctx)

    ctx.client.sql("""
    CREATE TABLE test_macros AS
    SELECT
        100::BIGINT AS gas_price,
        200::BIGINT AS receipt_gas_used,
        0 AS zero,
        50 as fifty
    """)

    # Use raw sql.
    result = ctx.client.sql("""
    SELECT
        gas_price * receipt_gas_used,
        wei_to_eth(gas_price * receipt_gas_used) AS ans_eth,
        wei_to_gwei(gas_price * receipt_gas_used) AS ans_gwei,
        safe_div(receipt_gas_used, fifty) AS ans_division_ok,
        safe_div(receipt_gas_used, zero) AS ans_division_err
    FROM test_macros
    """)
    actual = result.fetchall()

    expected = [(20000, Decimal("2.00000E-14"), Decimal("0.0000200000"), 4.0, None)]
    assert actual == expected


def test_gwei_to_eth():
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
    SELECT
        gwei_to_eth(1) AS m1,
        gwei_to_eth(10550003221200) as m2,
        gwei_to_eth(600123.54::DECIMAL(28, 2)) as m3,
        gwei_to_eth(500000.554) as m4
     """).fetchall()[0]

    expected = (
        Decimal("1.0E-9"),
        Decimal("10550.0032212000"),
        Decimal("0.00060012354"),
        Decimal("0.000500000554"),
    )
    assert actual == expected


def test_epoch_to_hour_and_date():
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
    SELECT
        100::BIGINT AS timestamp,
        epoch_to_hour(100::INT) as hour1,
        epoch_to_hour(3700::INT) as hour2,
        epoch_to_hour(1731176747) as hour3,
        epoch_to_day(100::INT) as date1,
        epoch_to_day(3700::INT) as date2,
        epoch_to_day(1731176747) as date3
     """).fetchall()[0]

    expected = (
        100,
        datetime.datetime(1970, 1, 1, 0, 0),
        datetime.datetime(1970, 1, 1, 1, 0),
        datetime.datetime(2024, 11, 9, 18, 0),
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 1),
        datetime.date(2024, 11, 9),
    )
    assert actual == expected


def test_micro():
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
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
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
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
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
    SELECT
        hexstr_bytelen('0x3d602d80600a3d3981f3363d3d373d3d3d363d739ec1c3dcf667f2035fb4cd2eb42a1566fd54d2b75af43d82803e903d91602b57fd5bf3') AS m1,
        hexstr_bytelen('0x3d60')
     """).fetchall()[0]

    expected = (
        55,
        2,
    )
    assert actual == expected


def test_hexstr_byte_related():
    ctx = init_client()
    create_duckdb_macros(ctx)

    test_inputs = [
        "0x3006",
        "0x3f00",
        "0x3d602d80600a3d3981f3363d3d373d3d3d363d739ec1c3dcf667f2035fb4cd2eb42a1566fd54d2b75af43d82803e903d91602b57fd5bf3",
        "0x3d60",
        "0x000000",
    ]

    actual = []
    for test in test_inputs:
        result = ctx.client.sql(f"""
            SELECT
                hexstr_bytelen('{test}') as len,
                hexstr_zero_bytes('{test}') as zero,
                hexstr_calldata_gas('{test}') as calldata_gas
            """).fetchall()[0]
        actual.append(result)

    assert actual == [
        (2, 0, 32),
        (2, 1, 20),
        (55, 0, 880),
        (2, 0, 32),
        (3, 3, 12),
    ]


def test_hexstr_method_id():
    ctx = init_client()
    create_duckdb_macros(ctx)

    actual = ctx.client.sql("""
    SELECT
        hexstr_method_id('0x3d602d80600a3d3981f3363d3d373d3d3d363d739ec1c3dcf667f2035fb4cd2eb42a1566fd54d2b75af43d82803e903d91602b57fd5bf3') AS m1,
     """).fetchall()[0]

    expected = ("0x3d602d80",)
    assert actual == expected


def test_hextr_bytes_py_udf():
    ctx = init_client()
    create_duckdb_macros(ctx)

    test_inputs = [
        "0x3006",
        "0x3f00",
        "0x3d602d80600a3d3981f3363d3d373d3d3d363d739ec1c3dcf667f2035fb4cd2eb42a1566fd54d2b75af43d82803e903d91602b57fd5bf3",
        "0x3d60",
        "0x000000",
    ]

    actual = []
    for test in test_inputs:
        result = ctx.client.sql(f"""
            SELECT
                hexstr_bytelen('{test}') as len,
                hexstr_zero_bytes('{test}') as zero,
            """).fetchall()[0]
        actual.append(result)

    assert actual == [
        (2, 0),
        (2, 1),
        (55, 0),
        (2, 0),
        (3, 3),
    ]


def test_trace_address_helpers():
    ctx = init_client()
    create_duckdb_macros(ctx)

    test_inputs = ["", "0", "0,0", "0,1", "0,10,0", "0,10,0,0", "0,2"]

    actual = []
    for test in test_inputs:
        result = ctx.client.sql(f"""
            SELECT
                trace_address_depth('{test}') as depth,
                trace_address_parent('{test}') as parent,
            """).fetchall()[0]
        actual.append(result)

    assert actual == [
        (0, "root"),
        (1, "first"),
        (2, "0"),
        (2, "0"),
        (3, "0,10"),
        (4, "0,10,0"),
        (2, "0"),
    ]
