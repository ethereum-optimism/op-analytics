# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from op_coreutils.testutils.inputdata import InputTestData

from op_analytics.cli.subcommands.pulls import defillama

TESTDATA = InputTestData.at(__file__)

# Sample data resembling the API response
current_timestamp = int(datetime.now(timezone.utc).timestamp())

sample_data = {
    "pegType": "peggedUSD",
    "chainBalances": {
        "Ethereum": {
            "tokens": [
                {
                    "date": current_timestamp,
                    "circulating": {"peggedUSD": 1000000},
                    "bridgedTo": {"peggedUSD": 200000},
                    "minted": {"peggedUSD": 1200000},
                    "unreleased": {"peggedUSD": 0},
                },
                {
                    "date": current_timestamp - 86400 * 2,  # 2 days ago
                    "circulating": {"peggedUSD": 900000},
                    "bridgedTo": {"peggedUSD": 180000},
                    "minted": {"peggedUSD": 1080000},
                    "unreleased": {"peggedUSD": 0},
                },
            ]
        }
    },
    "id": "sample-stablecoin",
    "name": "Sample Stablecoin",
    "address": "0xSampleAddress",
    "symbol": "SSC",
    "url": "https://samplestablecoin.com",
    "pegMechanism": "fiat-backed",
    "description": "A sample stablecoin for testing.",
    "mintRedeemDescription": "Mint and redeem instructions.",
    "onCoinGecko": True,
    "gecko_id": "sample-stablecoin",
    "cmcId": "12345",
    "priceSource": "coingecko",
    "twitter": "@samplestablecoin",
    "price": 1.00,
}


@pytest.fixture
def mock_sample_data():
    return sample_data


def test_process_breakdown_stables(mock_sample_data):
    # Test with default days=30
    result_df, metadata = defillama.process_breakdown_stables(mock_sample_data)
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 2  # Two data points within 30 days
    assert metadata["id"] == "sample-stablecoin"
    assert metadata["name"] == "Sample Stablecoin"

    # Test filtering with days=1
    result_df_recent, _ = defillama.process_breakdown_stables(mock_sample_data, days=1)
    assert len(result_df_recent) == 1  # Only the most recent data point

    # Check data correctness
    assert result_df["circulating"][0] == 1000000
    assert result_df["bridged_to"][0] == 200000
    assert result_df["minted"][0] == 1200000
    assert result_df["unreleased"][0] == 0


@patch("op_analytics.cli.subcommands.pulls.defillama.get_data")  # Correct mock path
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_unpartitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_partition_static")
@patch("op_analytics.cli.subcommands.pulls.defillama.upsert_partitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.new_session")
def test_pull_stables(
    mock_new_session,
    mock_upsert_partitioned_table,
    mock_overwrite_partition_static,
    mock_overwrite_unpartitioned_table,
    mock_get_data,
    mock_sample_data,
):
    # Mock the session
    mock_session = MagicMock()
    mock_new_session.return_value = mock_session

    # Mock get_data to return sample summary and sample data
    mock_get_data.side_effect = [
        {
            "peggedAssets": [
                {"id": "sample-stablecoin", "name": "Sample Stablecoin"},
                {"id": "another-stablecoin", "name": "Another Stablecoin"},
            ]
        },
        mock_sample_data,  # For the stablecoin breakdown
    ]

    # Call the function under test
    result = defillama.pull_stables(stablecoin_ids=["sample-stablecoin"], days=30)

    # Assertions
    assert "metadata" in result
    assert "breakdown" in result
    assert isinstance(result["metadata"], pl.DataFrame)
    assert isinstance(result["breakdown"], pl.DataFrame)
    assert len(result["metadata"]) == 1
    assert len(result["breakdown"]) == 2

    # Check that BigQuery functions were called
    assert mock_overwrite_unpartitioned_table.called
    assert mock_overwrite_partition_static.called
    assert mock_upsert_partitioned_table.called

    # Verify that get_data was called twice (summary and breakdown)
    assert mock_get_data.call_count == 2

    # Check that upsert_partitioned_table was called with correct parameters
    args, kwargs = mock_upsert_partitioned_table.call_args
    assert "dataset" in kwargs
    assert "table_name" in kwargs
    assert "unique_keys" in kwargs
    assert "partition_dt" in kwargs
    assert kwargs["unique_keys"] == ["dt", "id", "chain"]
