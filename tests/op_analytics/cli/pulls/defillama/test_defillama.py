# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from op_coreutils.testutils.inputdata import InputTestData

from op_analytics.cli.subcommands.pulls import defillama

TESTDATA = InputTestData.at(__file__)

# Current timestamp for testing
current_timestamp = int(datetime.now(timezone.utc).timestamp())

# Sample data resembling the API response
sample_summary = {
    "peggedAssets": [
        {"id": "sample-stablecoin", "name": "Sample Stablecoin"},
        {"id": "another-stablecoin", "name": "Another Stablecoin"},
    ]
}

sample_breakdown_data = {
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

another_sample_breakdown_data = {
    # Similar structure with required fields
    "pegType": "peggedUSD",
    "chainBalances": {
        "Binance Smart Chain": {
            "tokens": [
                {
                    "date": current_timestamp,
                    "circulating": {"peggedUSD": 500000},
                    "bridgedTo": {"peggedUSD": 100000},
                    "minted": {"peggedUSD": 600000},
                    "unreleased": {"peggedUSD": 0},
                }
            ]
        }
    },
    "id": "another-stablecoin",
    "name": "Another Stablecoin",
    "address": "0xAnotherSampleAddress",
    "symbol": "ASC",
    "url": "https://anotherstablecoin.com",
    "pegMechanism": "algorithmic",
    "description": "Another sample stablecoin for testing.",
    "mintRedeemDescription": "Mint and redeem instructions.",
    "onCoinGecko": True,
    "gecko_id": "another-stablecoin",
    "cmcId": "67890",
    "priceSource": "coingecko",
    "twitter": "@anotherstablecoin",
    "price": 1.00,
}


def test_process_breakdown_stables():
    # Test with valid data
    result_df, metadata = defillama.process_breakdown_stables(sample_breakdown_data)
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 2  # Two data points within default 30 days
    assert metadata["id"] == "sample-stablecoin"
    assert metadata["name"] == "Sample Stablecoin"

    # Test filtering with days=1 (should only include recent data)
    result_df_recent, _ = defillama.process_breakdown_stables(
        sample_breakdown_data, days=1
    )
    assert len(result_df_recent) == 1  # Only the most recent data point

    # Check data correctness
    assert result_df["circulating"][0] == 1000000
    assert result_df["bridged_to"][0] == 200000
    assert result_df["minted"][0] == 1200000
    assert result_df["unreleased"][0] == 0

    # Test with missing required fields (should raise ValueError)
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["pegType"]
    with pytest.raises(ValueError) as excinfo:
        defillama.process_breakdown_stables(incomplete_data)
    assert "Missing required fields" in str(excinfo.value)


@patch("op_analytics.cli.subcommands.pulls.defillama.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_unpartitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_partition_static")
@patch("op_analytics.cli.subcommands.pulls.defillama.upsert_partitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.new_session")
def test_pull_stables_single_stablecoin(
    mock_new_session,
    mock_upsert_partitioned_table,
    mock_overwrite_partition_static,
    mock_overwrite_unpartitioned_table,
    mock_get_data,
):
    # Mock the session
    mock_session = MagicMock()
    mock_new_session.return_value = mock_session

    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_breakdown_data,  # Breakdown data for 'sample-stablecoin'
    ]

    # Call the function under test
    result = defillama.pull_stables(stablecoin_ids=["sample-stablecoin"], days=30)

    # Assertions
    assert "metadata" in result
    assert "breakdown" in result
    assert isinstance(result["metadata"], pl.DataFrame)
    assert isinstance(result["breakdown"], pl.DataFrame)
    assert len(result["metadata"]) == 1  # Only 'sample-stablecoin'
    assert len(result["breakdown"]) == 2  # Two data points

    # Check that BigQuery functions were called
    mock_overwrite_unpartitioned_table.assert_called_once()
    mock_overwrite_partition_static.assert_called_once()
    assert mock_upsert_partitioned_table.call_count == len(
        result["breakdown"]["dt"].unique()
    )  # Called for each unique date

    # Verify that get_data was called twice (summary and breakdown)
    assert mock_get_data.call_count == 2

    # Check that upsert_partitioned_table was called with correct parameters
    for call in mock_upsert_partitioned_table.call_args_list:
        args, kwargs = call
        assert "dataset" in kwargs
        assert "table_name" in kwargs
        assert "unique_keys" in kwargs
        assert "partition_dt" in kwargs
        assert kwargs["unique_keys"] == ["dt", "id", "chain"]


@patch("op_analytics.cli.subcommands.pulls.defillama.get_data")
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_unpartitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.overwrite_partition_static")
@patch("op_analytics.cli.subcommands.pulls.defillama.upsert_partitioned_table")
@patch("op_analytics.cli.subcommands.pulls.defillama.new_session")
def test_pull_stables_multiple_stablecoins(
    mock_new_session,
    mock_upsert_partitioned_table,
    mock_overwrite_partition_static,
    mock_overwrite_unpartitioned_table,
    mock_get_data,
):
    # Mock the session
    mock_session = MagicMock()
    mock_new_session.return_value = mock_session

    # Mock get_data to return sample summary and breakdown data for both stablecoins
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_breakdown_data,  # Breakdown data for 'sample-stablecoin'
        another_sample_breakdown_data,  # Breakdown data for 'another-stablecoin'
    ]

    # Call the function under test without specifying stablecoin_ids (process all)
    result = defillama.pull_stables(days=30)

    # Assertions
    assert "metadata" in result
    assert "breakdown" in result
    assert isinstance(result["metadata"], pl.DataFrame)
    assert isinstance(result["breakdown"], pl.DataFrame)
    assert len(result["metadata"]) == 2  # Both stablecoins
    assert len(result["breakdown"]) >= 3  # Data points from both stablecoins

    # Verify that get_data was called three times (summary and two breakdowns)
    assert mock_get_data.call_count == 3

    # Check that BigQuery functions were called
    mock_overwrite_unpartitioned_table.assert_called_once()
    mock_overwrite_partition_static.assert_called_once()
    assert mock_upsert_partitioned_table.call_count == len(
        result["breakdown"]["dt"].unique()
    )  # Called for each unique date

    # Check that metadata contains both stablecoins
    assert set(result["metadata"]["id"].to_list()) == {
        "sample-stablecoin",
        "another-stablecoin",
    }

    # Check that breakdown contains data from both stablecoins
    assert set(result["breakdown"]["id"].unique()) == {
        "sample-stablecoin",
        "another-stablecoin",
    }


def test_pull_stables_no_valid_ids():
    # Test pull_stables with invalid stablecoin_ids (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = sample_summary
        with pytest.raises(ValueError) as excinfo:
            defillama.pull_stables(stablecoin_ids=["non-existent-stablecoin"], days=30)
        assert "No valid stablecoin IDs provided." in str(excinfo.value)


def test_pull_stables_missing_pegged_assets():
    # Test pull_stables when 'peggedAssets' is missing (should raise KeyError)
    incomplete_summary = {"someOtherKey": []}
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = incomplete_summary
        with pytest.raises(KeyError) as excinfo:
            defillama.pull_stables(days=30)
        assert "The 'peggedAssets' key is missing from the summary data." in str(
            excinfo.value
        )


def test_process_breakdown_stables_empty_balances():
    # Test process_breakdown_stables with empty 'chainBalances' (should return empty DataFrame)
    data_with_empty_balances = sample_breakdown_data.copy()
    data_with_empty_balances["chainBalances"] = {}
    result_df, metadata = defillama.process_breakdown_stables(data_with_empty_balances)
    assert result_df.is_empty()
    assert metadata["id"] == "sample-stablecoin"


def test_process_breakdown_stables_missing_mandatory_metadata():
    # Test process_breakdown_stables with missing mandatory metadata fields (should raise KeyError)
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["name"]  # Remove a mandatory field
    with pytest.raises(KeyError) as excinfo:
        defillama.process_breakdown_stables(incomplete_data)
    assert "Missing required metadata field: 'name'" in str(excinfo.value)


def test_process_breakdown_stables_optional_metadata():
    # Test process_breakdown_stables with missing optional metadata fields
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["description"]  # Remove an optional field
    result_df, metadata = defillama.process_breakdown_stables(incomplete_data)
    assert "description" in metadata
    assert metadata["description"] is None  # Should be None if missing


def test_pull_stables_empty_metadata_df():
    # Test pull_stables when metadata_df is empty (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.get_data"
    ) as mock_get_data:
        mock_get_data.side_effect = [
            sample_summary,  # Summary data
            # Data that causes process_breakdown_stables to return empty DataFrames
            {
                "pegType": "peggedUSD",
                "chainBalances": {},
                "id": "sample-stablecoin",
                "name": "Sample Stablecoin",
                "address": "0xSampleAddress",
                "symbol": "SSC",
                "url": "https://samplestablecoin.com",
                "pegMechanism": "fiat-backed",
            },
        ]
        with pytest.raises(ValueError) as excinfo:
            defillama.pull_stables(stablecoin_ids=["sample-stablecoin"], days=30)
        assert "metadata_df is empty" in str(excinfo.value)


def test_pull_stables_empty_breakdown_df():
    # Test pull_stables when breakdown_df is empty (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.get_data"
    ) as mock_get_data:
        mock_get_data.side_effect = [
            sample_summary,  # Summary data
            {
                "pegType": "peggedUSD",
                "chainBalances": {},
                "id": "sample-stablecoin",
                "name": "Sample Stablecoin",
                "address": "0xSampleAddress",
                "symbol": "SSC",
                "url": "https://samplestablecoin.com",
                "pegMechanism": "fiat-backed",
            },
        ]
        with pytest.raises(ValueError) as excinfo:
            defillama.pull_stables(stablecoin_ids=["sample-stablecoin"], days=30)
        assert "metadata_df is empty." in str(excinfo.value)
