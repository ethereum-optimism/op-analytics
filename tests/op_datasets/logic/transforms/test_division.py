from decimal import Decimal

import polars as pl


def test_division_in_polars():
    data = {"total_gas": [161008631645532]}
    df = pl.DataFrame(data, schema={"total_gas": pl.Int64})
    pl.SQLContext(frames={"df": df})

    ans = pl.sql(
        """
        SELECT 
            total_gas, 
            CAST(total_gas AS DECIMAL(38, 18)) / CAST(1000000000000000000 AS DECIMAL(18, 0)) AS total_gas_converted
        FROM df
        """
    ).collect()

    actual = ans.to_dicts()[0]
    expected = {
        "total_gas": 161008631645532,
        "total_gas_converted": Decimal("0.0001610086316455320000"),
    }
    assert actual == expected
