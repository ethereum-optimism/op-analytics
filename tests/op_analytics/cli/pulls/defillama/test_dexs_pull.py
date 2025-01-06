import json
import os
from unittest.mock import patch

import polars as pl

from op_analytics.cli.subcommands.pulls.defillama.dex_volume_fees_revenue import pull_dex_dataframes

# Module path to patch data retrieval functions
MODULE = "op_analytics.cli.subcommands.pulls.defillama.dex_volume_fees_revenue"

EXPECTED_PROTOCOLS_DF_SCHEMA = {
    "defillamaId": pl.String,
    "name": pl.String,
    "displayName": pl.String,
    "module": pl.String,
    "category": pl.String,
    "logo": pl.String,
    "chains": pl.List(pl.String),
    "protocolType": pl.String,
    "methodologyURL": pl.String,
    "methodology": pl.List(pl.Struct({"key": pl.String, "value": pl.String})),
    "latestFetchIsOk": pl.Boolean,
    "slug": pl.String,
    "id": pl.String,
    "parentProtocol": pl.String,
    "total24h": pl.Int64,
    "total48hto24h": pl.Int64,
    "total7d": pl.Int64,
    "total14dto7d": pl.Int64,
    "total60dto30d": pl.Int64,
    "total30d": pl.Int64,
    "total1y": pl.Int64,
    "totalAllTime": pl.Float64,
    "average1y": pl.Float64,
    "change_1d": pl.Float64,
    "change_7d": pl.Float64,
    "change_1m": pl.Float64,
    "change_7dover7d": pl.Float64,
    "change_30dover30d": pl.Float64,
    "breakdown24h": pl.List(pl.Struct({"chain": pl.String, "name": pl.String, "value": pl.Int64})),
}


def read_mock_data(path):
    path = os.path.join(os.path.dirname(__file__), f"mockdata/{path}")
    with open(path, "r") as f:
        return json.load(f)


def mock_get_data(session, url):
    if url.endswith("dailyVolume"):
        return read_mock_data("dexs_daily_volume_summary_response.json")

    if url.endswith("dailyFees"):
        return read_mock_data("fees_daily_fees_summary_response.json")

    if url.endswith("dailyRevenue"):
        return read_mock_data("fees_daily_revenue_summary_response.json")

    raise NotImplementedError(f"Mock data not implemented for {url}")


def mock_get_chain_responses(session, summary_response, data_type):
    if data_type == "dailyVolume":
        return read_mock_data("chain_daily_volume.json")
    if data_type == "dailyFees":
        return read_mock_data("chain_daily_fees.json")
    if data_type == "dailyRevenue":
        return read_mock_data("chain_daily_revenue.json")

    raise NotImplementedError(f"Mock data not implemented for {data_type}")


@patch(f"{MODULE}.write")
def test(mock_write):
    with (
        patch(f"{MODULE}.get_data", new=mock_get_data),
        patch(f"{MODULE}.get_chain_responses", new=mock_get_chain_responses),
    ):
        pull_dex_dataframes()

    assert len(mock_write.call_args_list) == 1

    crypto_df = mock_write.call_args_list[0].kwargs["crypto_df"]
    chain_df = mock_write.call_args_list[0].kwargs["chain_df"]
    chain_protocol_df = mock_write.call_args_list[0].kwargs["chain_protocol_df"]
    volume_protocols_df = mock_write.call_args_list[0].kwargs["volume_protocols_df"]
    fees_protocols_df = mock_write.call_args_list[0].kwargs["fees_protocols_df"]
    revenue_protocols_df = mock_write.call_args_list[0].kwargs["revenue_protocols_df"]

    assert crypto_df.columns == [
        "dt",
        "total_volume_usd",
        "total_fees_usd",
        "total_revenue_usd",
    ]
    assert chain_df.columns == [
        "dt",
        "chain",
        "total_volume_usd",
        "total_fees_usd",
        "total_revenue_usd",
    ]
    assert chain_protocol_df.columns == [
        "dt",
        "chain",
        "protocol",
        "total_volume_usd",
        "total_fees_usd",
        "total_revenue_usd",
    ]

    assert volume_protocols_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
    assert fees_protocols_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
    assert revenue_protocols_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
