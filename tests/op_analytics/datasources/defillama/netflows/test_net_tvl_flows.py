import datetime
import os

import numpy as np
import polars as pl

from op_analytics.datasources.defillama.protocolstvlenrich.calculate import calculate_net_flows

DIRECTORY = os.path.dirname(__file__)


def mock_data(data):
    return pl.DataFrame(
        data,
        schema={
            "dt": pl.String,
            "chain": pl.String,
            "protocol_slug": pl.String,
            "token": pl.String,
            "app_token_tvl": pl.Float64,
            "app_token_tvl_usd": pl.Float64,
        },
    ).with_columns(dt=pl.col("dt").str.to_date(format="%Y-%m-%d"))


def mock_lookback_data():
    return pl.DataFrame(
        [
            {
                "dt": "2024-11-23",
                "lookback": 90,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 518.30507,
            },
            {
                "dt": "2024-12-23",
                "lookback": 60,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 828.61877,
            },
            {
                "dt": "2025-01-24",
                "lookback": 28,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1164.26412,
            },
            {
                "dt": "2025-02-07",
                "lookback": 14,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1338.28103,
            },
            {
                "dt": "2025-02-04",
                "lookback": 7,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1301.51879,
            },
            {
                "dt": "2025-02-20",
                "lookback": 1,
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1464.44488,
            },
        ]
    )


def test_calculate_net_flow():
    computedate_df = mock_data(
        [
            {
                "dt": "2025-02-21",
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1460.6662,
                "app_token_tvl_usd": 143836182.37188,
            }
        ]
    )
    lookback_df = mock_lookback_data()

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
    }

    previous = [m01day, m14day, m60day]
    assert previous == [
        {
            "dt": "2025-02-20",
            "lookback": 1,
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1464.44488,
        },
        {
            "dt": "2025-02-07",
            "lookback": 14,
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1338.28103,
        },
        {
            "dt": "2024-12-23",
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
        "app_token_tvl_usd": 143836182.37188,
        "usd_conversion_rate": 98472.99976673658,
        "app_token_tvl_1d": 1464.44488,
        "app_token_tvl_14d": 1338.28103,
        "app_token_tvl_60d": 828.61877,
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


def test_calculate_net_flow_no_lookback():
    """In this test the flow is calculated at 22d, for which we do not have data.

    We expect teh tvl_22d and flow_22d values to be NULL."""
    computedate_df = mock_data(
        [
            {
                "dt": "2025-02-21",
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": 1460.6662,
                "app_token_tvl_usd": 143836182.37188,
            }
        ]
    )
    lookback_df = mock_lookback_data()

    ans = calculate_net_flows(computedate_df, lookback_df, [22]).to_dicts()

    assert ans == [
        {
            "dt": datetime.date(2025, 2, 21),
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": 1460.6662,
            "app_token_tvl_usd": 143836182.37188,
            "usd_conversion_rate": 98472.99976673658,
            "app_token_tvl_22d": None,
            "net_token_flow_22d": None,
        }
    ]


def test_calculate_net_flow_null_tvl():
    """In this case both tvl and tvl_use are NULL.

    We expect the flow result to be NULL also.
    """
    computedate_df = mock_data(
        [
            {
                "dt": "2025-02-21",
                "chain": "Base",
                "protocol_slug": "aave-v3",
                "token": "CBBTC",
                "app_token_tvl": None,
                "app_token_tvl_usd": None,
            }
        ]
    )
    lookback_df = mock_lookback_data()

    ans = calculate_net_flows(computedate_df, lookback_df, [14]).to_dicts()

    assert ans == [
        {
            "dt": datetime.date(2025, 2, 21),
            "chain": "Base",
            "protocol_slug": "aave-v3",
            "token": "CBBTC",
            "app_token_tvl": None,
            "app_token_tvl_usd": None,
            "usd_conversion_rate": None,
            "app_token_tvl_14d": 1338.28103,
            "net_token_flow_14d": None,
        }
    ]


def test_calculate_net_flow_zero_tvl():
    """In this test the conversion_rate is 'inf' because tvl_usd is non-zero.

    We coerce the converstion rate from 'inf' to 0. Which results in a flow
    value equal to the current tvl_usd."""

    computedate_df = mock_data(
        [
            {
                "dt": "2025-02-23",
                "chain": "Base",
                "protocol_slug": "charm-finance-v2",
                "token": "AXLWBTC",
                # This values will result in an 'inf' USD conversion rate.
                "app_token_tvl": 0,
                "app_token_tvl_usd": 0.44066,
            }
        ]
    )
    lookback_df = pl.DataFrame(
        [
            {
                "dt": "2025-02-16",
                "lookback": 7,
                "chain": "Base",
                "protocol_slug": "charm-finance-v2",
                "token": "AXLWBTC",
                "app_token_tvl": 2e-05,
            },
        ]
    )

    ans = calculate_net_flows(computedate_df, lookback_df, [7]).to_dicts()
    assert ans == [
        {
            "dt": datetime.date(2025, 2, 23),
            "chain": "Base",
            "protocol_slug": "charm-finance-v2",
            "token": "AXLWBTC",
            #
            "app_token_tvl": 0.0,
            "app_token_tvl_usd": 0.44066,
            "usd_conversion_rate": 0.0,
            #
            "app_token_tvl_7d": 2e-05,
            "net_token_flow_7d": 0.44066,
        }
    ]


def test_calculate_net_flow_zero_tvl_and_tvl_usd():
    """In this test the conversion_rate comes up as NaN because tvl and tvl_usde are both zero.

    We expect the net flow to be assigned as zero.
    """

    computedate_df = mock_data(
        [
            {
                "dt": "2025-02-23",
                "chain": "Base",
                "protocol_slug": "charm-finance-v2",
                "token": "AXLWBTC",
                "app_token_tvl": 0,
                "app_token_tvl_usd": 0,
            }
        ]
    )
    lookback_df = pl.DataFrame(
        [
            {
                "dt": "2025-02-16",
                "lookback": 7,
                "chain": "Base",
                "protocol_slug": "charm-finance-v2",
                "token": "AXLWBTC",
                "app_token_tvl": 0,
            },
        ]
    )

    ans = calculate_net_flows(computedate_df, lookback_df, [7]).to_dicts()
    assert np.isnan(ans[0].pop("usd_conversion_rate"))
    assert ans == [
        {
            "dt": datetime.date(2025, 2, 23),
            "chain": "Base",
            "protocol_slug": "charm-finance-v2",
            "token": "AXLWBTC",
            #
            "app_token_tvl": 0.0,
            "app_token_tvl_usd": 0.0,
            #
            "app_token_tvl_7d": 0.0,
            "net_token_flow_7d": 0.0,
        }
    ]
