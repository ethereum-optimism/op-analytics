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
    ],
    "pegType": "peggedUSD",
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


@pytest.mark.parametrize(
    "data,days,expected_rows",
    [
        (sample_breakdown_data, 30, 2),
        (sample_breakdown_data, 1, 1),  # Only recent data
    ],
)
def test_process_breakdown_stables(data, days, expected_rows):
    result_df, metadata_df = defillama.process_breakdown_stables(data, days=days)
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == expected_rows
    assert metadata_df["id"].to_list()[0] == data["id"]
    assert metadata_df["name"].to_list()[0] == data["name"]

    if days == 30:
        assert result_df["circulating"].to_list()[0] == 1000000

    # Missing required fields check
    incomplete_data = data.copy()
    del incomplete_data["pegType"]
    with pytest.raises(KeyError):
        defillama.process_breakdown_stables(incomplete_data)


@patch("op_analytics.cli.subcommands.pulls.defillama.get_data")
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
):
    mock_session = MagicMock()
    mock_new_session.return_value = mock_session

    mock_get_data.side_effect = [
        sample_summary,
        sample_breakdown_data,
        another_sample_breakdown_data,
    ]

    result = defillama.pull_stables(days=30)

    # Assertions on the result dictionary
    assert "metadata" in result
    assert "breakdown" in result
    assert isinstance(result["metadata"], pl.DataFrame)
    assert isinstance(result["breakdown"], pl.DataFrame)
    assert len(result["metadata"]) == 2
    assert len(result["breakdown"]) >= 3

    mock_overwrite_unpartitioned_table.assert_called_once()
    mock_overwrite_partition_static.assert_called_once()
    mock_upsert_partitioned_table.assert_called_once()


@pytest.mark.parametrize(
    "stablecoin_ids,raises_exception",
    [(["non-existent-stablecoin"], True), (None, False)],
)
def test_pull_stables_no_valid_ids(stablecoin_ids, raises_exception):
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.get_data"
    ) as mock_get_data:
        # Creating mock data to ensure all required fields are present
        sample_data_with_full_fields = {
            "peggedAssets": [
                {"id": "sample-stablecoin", "name": "Sample Stablecoin"},
                {"id": "another-stablecoin", "name": "Another Stablecoin"},
            ],
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
                        }
                    ]
                }
            },
            "id": "sample-stablecoin",
            "name": "Sample Stablecoin",
            "address": "0xSampleAddress",
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

        # Mock get_data to return the corrected sample data
        mock_get_data.return_value = sample_data_with_full_fields

        if raises_exception:
            with pytest.raises(ValueError):
                defillama.pull_stables(stablecoin_ids=stablecoin_ids, days=30)
        else:
            # We expect no exceptions, so we just call the function
            result = defillama.pull_stables(stablecoin_ids=stablecoin_ids, days=30)

            # Assertions to ensure the expected keys are present in the result
            assert "metadata" in result
            assert "breakdown" in result
            assert isinstance(result["metadata"], pl.DataFrame)
            assert isinstance(result["breakdown"], pl.DataFrame)


@patch("op_analytics.cli.subcommands.pulls.defillama.get_data")
def test_pull_stables_missing_pegged_assets(mock_get_data):
    incomplete_summary = {"someOtherKey": []}
    mock_get_data.return_value = incomplete_summary
    with pytest.raises(KeyError):
        defillama.pull_stables(days=30)


@pytest.mark.parametrize(
    "data,field",
    [
        (sample_breakdown_data, "name"),
    ],
)
def test_process_breakdown_stables_missing_mandatory_metadata(data, field):
    incomplete_data = data.copy()
    del incomplete_data[field]
    with pytest.raises(KeyError):
        defillama.process_breakdown_stables(incomplete_data)


@pytest.mark.parametrize("data,field", [(sample_breakdown_data, "description")])
def test_process_breakdown_stables_optional_metadata(data, field):
    incomplete_data = data.copy()
    del incomplete_data[field]
    result_df, metadata_df = defillama.process_breakdown_stables(incomplete_data)
    assert field in metadata_df.columns
    assert metadata_df[field][0] is None
