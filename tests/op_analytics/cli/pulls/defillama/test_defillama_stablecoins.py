from unittest.mock import patch

import pytest
from op_analytics.coreutils.testutils.inputdata import InputTestData


from op_analytics.cli.subcommands.pulls.defillama import stablecoins

TESTDATA = InputTestData.at(__file__)


# Sample data resembling the API response
sample_summary = {
    "peggedAssets": [
        {"id": "sample-stablecoin", "name": "Sample Stablecoin", "symbol": "SSC"},
        {"id": "another-stablecoin", "name": "Another Stablecoin", "symbol": "ANOTHER"},
    ]
}

sample_breakdown_data = {
    "pegType": "peggedUSD",
    "chainBalances": {
        "Ethereum": {
            "tokens": [
                {
                    "date": 1730923615,
                    "circulating": {"peggedUSD": 1000000},
                    "bridgedTo": {"peggedUSD": 200000},
                    "minted": {"peggedUSD": 1200000},
                    "unreleased": {"peggedUSD": 0},
                },
                {
                    "date": 1730750815,  # 2 days ago
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
    "onCoinGecko": "true",
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
                    "date": 1730923615,
                    "circulating": {"peggedUSD": 167489338},
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
    "onCoinGecko": "true",
    "gecko_id": "another-stablecoin",
    "cmcId": "67890",
    "priceSource": "coingecko",
    "twitter": "@anotherstablecoin",
    "price": 1.00,
}


def test_process_breakdown_stables():
    # Test with valid data
    metadata, balances = stablecoins.single_stablecoin_balances(sample_breakdown_data)
    assert len(balances) == 2  # Two data points within default 30 days
    assert metadata["id"] == "sample-stablecoin"
    assert metadata["name"] == "Sample Stablecoin"

    # Check data correctness
    assert balances[0]["circulating"] == 1000000
    assert balances[0]["bridged_to"] == 200000
    assert balances[0]["minted"] == 1200000
    assert balances[0]["unreleased"] == 0

    # Test with missing required fields (should raise ValueError)
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["pegType"]
    with pytest.raises(KeyError) as excinfo:
        stablecoins.single_stablecoin_balances(incomplete_data)
    assert excinfo.value.args == ("pegType",)


@patch("op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data")
@patch("op_analytics.coreutils.partitioned.dailydata.PartitionedWriteManager.write")
def test_pull_stables_single_stablecoin(
    mock_write,
    mock_get_data,
):
    # Mock get_data to return sample summary and breakdown data
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_breakdown_data,  # Breakdown data for 'sample-stablecoin'
    ]

    # Call the function under test
    result = stablecoins.pull_stablecoins(symbols=["SSC"])

    # Assertions
    assert len(result.metadata_df) == 1  # Only 'sample-stablecoin'
    assert len(result.balances_df) == 2  # Two data points

    # Verify that get_data was called twice (summary and breakdown)
    assert mock_get_data.call_count == 2

    # Check that writer functions were called with correct parameters
    write_calls = [
        dict(
            dataset_name=_.kwargs["output_data"].root_path,
            df_columns=_.kwargs["output_data"].dataframe.columns,
            num_rows=len(_.kwargs["output_data"].dataframe),
        )
        for _ in mock_write.call_args_list
    ]
    assert write_calls == [
        {
            "dataset_name": "defillama/stablecoins_metadata_v1",
            "df_columns": [
                "id",
                "name",
                "address",
                "symbol",
                "url",
                "pegType",
                "pegMechanism",
                "description",
                "mintRedeemDescription",
                "onCoinGecko",
                "gecko_id",
                "cmcId",
                "priceSource",
                "twitter",
                "price",
                "dt",
            ],
            "num_rows": 1,
        },
        {
            "dataset_name": "defillama/stablecoins_balances_v1",
            "df_columns": [
                "id",
                "chain",
                "circulating",
                "bridged_to",
                "minted",
                "unreleased",
                "name",
                "symbol",
                "dt",
            ],
            "num_rows": 1,
        },
        {
            "dataset_name": "defillama/stablecoins_balances_v1",
            "df_columns": [
                "id",
                "chain",
                "circulating",
                "bridged_to",
                "minted",
                "unreleased",
                "name",
                "symbol",
                "dt",
            ],
            "num_rows": 1,
        },
    ]


@patch("op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data")
@patch("op_analytics.coreutils.partitioned.dailydata.PartitionedWriteManager.write")
def test_pull_stables_multiple_stablecoins(
    mock_write,
    mock_get_data,
):
    # Mock get_data to return sample summary and breakdown data for both stablecoins
    mock_get_data.side_effect = [
        sample_summary,  # Summary data
        sample_breakdown_data,  # Breakdown data for 'sample-stablecoin'
        another_sample_breakdown_data,  # Breakdown data for 'another-stablecoin'
    ]

    # Call the function under test without specifying stablecoin_ids (process all)
    result = stablecoins.pull_stablecoins()

    # Assertions
    assert len(result.metadata_df) == 2  # Both stablecoins
    assert len(result.balances_df) >= 3  # Data points from both stablecoins

    # Verify that get_data was called three times (summary and two breakdowns)
    assert mock_get_data.call_count == 3

    # Check that metadata contains both stablecoins
    assert set(result.metadata_df["id"].to_list()) == {
        "sample-stablecoin",
        "another-stablecoin",
    }

    # Check that breakdown contains data from both stablecoins
    assert set(result.balances_df["id"].unique()) == {
        "sample-stablecoin",
        "another-stablecoin",
    }

    # Check that writer functions were called with correct parameters
    write_calls = [
        dict(
            dataset_name=_.kwargs["output_data"].root_path,
            df_columns=_.kwargs["output_data"].dataframe.columns,
            num_rows=len(_.kwargs["output_data"].dataframe),
        )
        for _ in mock_write.call_args_list
    ]
    write_calls = sorted(write_calls, key=lambda x: (x["dataset_name"], x["num_rows"]))

    assert write_calls == [
        {
            "dataset_name": "defillama/stablecoins_balances_v1",
            "df_columns": [
                "id",
                "chain",
                "circulating",
                "bridged_to",
                "minted",
                "unreleased",
                "name",
                "symbol",
                "dt",
            ],
            "num_rows": 1,
        },
        {
            "dataset_name": "defillama/stablecoins_balances_v1",
            "df_columns": [
                "id",
                "chain",
                "circulating",
                "bridged_to",
                "minted",
                "unreleased",
                "name",
                "symbol",
                "dt",
            ],
            "num_rows": 2,
        },
        {
            "dataset_name": "defillama/stablecoins_metadata_v1",
            "df_columns": [
                "id",
                "name",
                "address",
                "symbol",
                "url",
                "pegType",
                "pegMechanism",
                "description",
                "mintRedeemDescription",
                "onCoinGecko",
                "gecko_id",
                "cmcId",
                "priceSource",
                "twitter",
                "price",
                "dt",
            ],
            "num_rows": 2,
        },
    ]


def test_pull_stables_no_valid_ids():
    # Test pull_stables with invalid stablecoin_ids (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = sample_summary
        with pytest.raises(ValueError) as excinfo:
            stablecoins.pull_stablecoins(symbols=["NOEXIST"])
        assert "No valid stablecoin IDs provided." in str(excinfo.value)


def test_pull_stables_missing_pegged_assets():
    # Test pull_stables when 'peggedAssets' is missing (should raise KeyError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data"
    ) as mock_get_data:
        mock_get_data.return_value = {}
        with pytest.raises(KeyError) as excinfo:
            stablecoins.pull_stablecoins()
        assert excinfo.value.args == ("peggedAssets",)


def test_process_breakdown_stables_empty_balances():
    # Test process_breakdown_stables with empty 'chainBalances' (should return empty DataFrame)
    data_with_empty_balances = sample_breakdown_data.copy()
    data_with_empty_balances["chainBalances"] = {}
    metadata, balances = stablecoins.single_stablecoin_balances(data_with_empty_balances)
    assert not balances
    assert metadata["id"] == "sample-stablecoin"


def test_process_breakdown_stables_missing_mandatory_metadata():
    # Test process_breakdown_stables with missing mandatory metadata fields (should raise KeyError)
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["name"]  # Remove a mandatory field
    with pytest.raises(KeyError) as excinfo:
        stablecoins.single_stablecoin_balances(incomplete_data)
    assert excinfo.value.args == ("name",)


def test_process_breakdown_stables_optional_metadata():
    # Test process_breakdown_stables with missing optional metadata fields
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["description"]  # Remove an optional field
    metadata, balances = stablecoins.single_stablecoin_balances(incomplete_data)
    assert "description" in metadata
    assert metadata["description"] is None  # Should be None if missing


def test_pull_stables_empty_metadata_df():
    # Test pull_stables when metadata_df is empty (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data"
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
            stablecoins.pull_stablecoins(symbols=["SSC"])
        assert excinfo.value.args == ("No balances for stablecoin=Sample Stablecoin",)


def test_pull_stables_empty_breakdown_df():
    # Test pull_stables when breakdown_df is empty (should raise ValueError)
    with patch(
        "op_analytics.cli.subcommands.pulls.defillama.stablecoins.get_data"
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
            stablecoins.pull_stablecoins(symbols=["SSC"])
        assert excinfo.value.args == ("No balances for stablecoin=Sample Stablecoin",)
