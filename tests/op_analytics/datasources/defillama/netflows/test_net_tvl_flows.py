import datetime
import json
import os

import polars as pl

from op_analytics.datasources.defillama.net_tvl_flows import calculate_net_flows


DIRECTORY = os.path.dirname(__file__)


def test_calculate_net_flow():
    with open(os.path.join(DIRECTORY, "aave-v3-CBBTC-tvl.json")) as fobj:
        df = pl.DataFrame(json.load(fobj))

    df = df.with_columns(dt=pl.col("dt").str.to_date(format="%Y-%m-%d"))

    first = df.sort(by=["dt"], descending=True).limit(1).to_dicts()[0]
    assert first == {
        "dt": datetime.date(2025, 2, 21),
        "chain": "Base",
        "protocol_slug": "aave-v3",
        "token": "CBBTC",
        "app_token_tvl": 1460.6662,
        "app_token_tvl_usd": 143836182.37188,
    }

    m01day = df.filter(
        pl.col("dt") == (datetime.date(2025, 2, 21) - datetime.timedelta(days=1))
    ).to_dicts()[0]
    m14day = df.filter(
        pl.col("dt") == (datetime.date(2025, 2, 21) - datetime.timedelta(days=14))
    ).to_dicts()[0]
    m60day = df.filter(
        pl.col("dt") == (datetime.date(2025, 2, 21) - datetime.timedelta(days=60))
    ).to_dicts()[0]

    previous = [m01day, m14day, m60day]
    assert previous == [
        {
            "dt": datetime.date(2025, 2, 20),
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1464.44488,
            "app_token_tvl_usd": 141266210.4519,
        },
        {
            "dt": datetime.date(2025, 2, 7),
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1338.28103,
            "app_token_tvl_usd": 129319434.15678,
        },
        {
            "dt": datetime.date(2024, 12, 23),
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 828.61877,
            "app_token_tvl_usd": 78642085.36695,
        },
    ]

    ans = calculate_net_flows(df=df, flow_days=[1, 14, 60])
    ans_first = ans.sort(by=["dt"], descending=True).limit(1).to_dicts()[0]
    assert ans_first == {
        "dt": datetime.date(2025, 2, 21),
        "chain": "Base",
        "protocol_slug": "aave-v3",
        "token": "CBBTC",
        "app_token_tvl": 1460.6662,
        "app_token_tvl_usd": 143836182.37188,
        "net_token_flow_1d": -372097.9547585845,
        "net_token_flow_14d": 12051634.816861987,
        "net_token_flow_60d": 62239606.42695643,
    }

    conversion_rate = ans_first["app_token_tvl_usd"] / ans_first["app_token_tvl"]

    net_flow_01day = first["app_token_tvl_usd"] - (m01day["app_token_tvl"] * conversion_rate)
    assert ans_first["net_token_flow_1d"] == net_flow_01day

    net_flow_14day = first["app_token_tvl_usd"] - (m14day["app_token_tvl"] * conversion_rate)
    assert ans_first["net_token_flow_14d"] == net_flow_14day

    net_flow_60day = first["app_token_tvl_usd"] - (m60day["app_token_tvl"] * conversion_rate)
    assert ans_first["net_token_flow_60d"] == net_flow_60day
