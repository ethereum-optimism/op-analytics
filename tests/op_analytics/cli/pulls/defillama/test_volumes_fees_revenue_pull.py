import json
import os
from unittest.mock import patch

import polars as pl

from op_analytics.datasources.defillama.volume_fees_revenue.execute import execute_pull

# Module path to patch data retrieval functions
MODULE = "op_analytics.datasources.defillama.volume_fees_revenue"

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
        execute_pull()

    assert len(mock_write.call_args_list) == 1

    write_kwargs = mock_write.call_args_list[0].kwargs
    chain_df = write_kwargs["chain_df"]
    breakdown_df = write_kwargs["breakdown_df"]
    dexs_protocols_metadata_df = write_kwargs["dexs_protocols_metadata_df"]
    fees_protocols_metadata_df = write_kwargs["fees_protocols_metadata_df"]
    revenue_protocols_metadata_df = write_kwargs["revenue_protocols_metadata_df"]

    assert chain_df.columns == [
        "dt",
        "chain",
        "total_volume_usd",
        "total_fees_usd",
        "total_revenue_usd",
    ]
    assert breakdown_df.columns == [
        "dt",
        "chain",
        "breakdown_name",
        "total_volume_usd",
        "total_fees_usd",
        "total_revenue_usd",
    ]

    assert dexs_protocols_metadata_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
    assert fees_protocols_metadata_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
    assert revenue_protocols_metadata_df.schema == EXPECTED_PROTOCOLS_DF_SCHEMA
