import json
import os

import polars as pl

from op_analytics.datasources.defillama.protocolstvl.protocol import ProtocolTVL


def test_parse_protocol():
    with open(os.path.join(os.path.dirname(__file__), "maverick-v2.json"), "r") as f:
        jsondata = json.load(f)

    data = ProtocolTVL.of(slug="maverick-v2", data=jsondata)

    data_token = data.token_tvl_df.filter(
        pl.col("chain") == "Base",
        pl.col("token") == "VIRTUAL",
        pl.col("dt") > "2024-12-28",
        pl.col("dt") < "2025-01-05",
    )

    assert data_token.to_dicts() == [
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2024-12-29",
            "token": "VIRTUAL",
            "app_token_tvl": 420.47531,
            "app_token_tvl_usd": 1442.23032,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2024-12-30",
            "token": "VIRTUAL",
            "app_token_tvl": 199.2854,
            "app_token_tvl_usd": 723.40602,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2024-12-31",
            "token": "VIRTUAL",
            "app_token_tvl": 4917.55649,
            "app_token_tvl_usd": 17063.92101,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2025-01-01",
            "token": "VIRTUAL",
            "app_token_tvl": 2308.05487,
            "app_token_tvl_usd": 9162.97784,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2025-01-02",
            "token": "VIRTUAL",
            "app_token_tvl": 21.49247,
            "app_token_tvl_usd": 96.93102,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2025-01-03",
            "token": "VIRTUAL",
            "app_token_tvl": 26.76353,
            "app_token_tvl_usd": 121.23878,
        },
        {
            "protocol_slug": "maverick-v2",
            "chain": "Base",
            "dt": "2025-01-04",
            "token": "VIRTUAL",
            "app_token_tvl": 5050.29485,
            "app_token_tvl_usd": 21766.77082,
        },
    ]
