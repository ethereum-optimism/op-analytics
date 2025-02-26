import datetime
import json
import os

import polars as pl

from op_analytics.datasources.defillama.net_tvl_flows.calculate import calculate_net_flows

DIRECTORY = os.path.dirname(__file__)


def test_calculate_net_flow():
    with open(os.path.join(DIRECTORY, "aave-v3-CBBTC-computedate.json")) as fobj:
        computedate_df = pl.DataFrame(json.load(fobj)).with_columns(
            dt=pl.col("dt").str.to_date(format="%Y-%m-%d")
        )

    with open(os.path.join(DIRECTORY, "aave-v3-CBBTC-lookback.json")) as fobj:
        lookback_df = pl.DataFrame(json.load(fobj))

    first = computedate_df.to_dicts()[0]
    m01day = lookback_df.filter(pl.col("lookback") == 1).to_dicts()[0]
    m14day = lookback_df.filter(pl.col("lookback") == 14).to_dicts()[0]
    m60day = lookback_df.filter(pl.col("lookback") == 60).to_dicts()[0]

    assert first == {
        "dt": datetime.date(2025, 2, 21),
        "chain": "Base",
        "protocol_slug": "aave-v3",
        "token": "CBBTC",
        "app_token_tvl": 1460.6662,
        "app_token_tvl_usd": 143836182.37188,
        "usd_conversion_rate": 98472.99976673658,
    }

    previous = [m01day, m14day, m60day]
    assert previous == [
        {
            "lookback": 1,
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1464.44488,
        },
        {
            "lookback": 14,
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1338.28103,
        },
        {
            "lookback": 60,
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 828.61877,
        },
    ]

    ans = calculate_net_flows(computedate_df, lookback_df, [1, 14, 60])

    ans_first = ans.sort(by=["dt"], descending=True).limit(1).to_dicts()[0]
    assert ans_first == {
        "dt": datetime.date(2025, 2, 21),
        "chain": "Base",
        "protocol_slug": "aave-v3",
        "token": "CBBTC",
        "app_token_tvl": 1460.6662,
        "usd_conversion_rate": 98472.99976673658,
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
