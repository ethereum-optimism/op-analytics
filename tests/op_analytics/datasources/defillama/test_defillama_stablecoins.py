from decimal import Decimal

import pytest
from op_analytics.coreutils.testutils.inputdata import InputTestData


from op_analytics.datasources.defillama.stablecoins.stablecoin import (
    single_stablecoin_balances,
    StableCoin,
)

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
    metadata, balances = single_stablecoin_balances(sample_breakdown_data)
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
        single_stablecoin_balances(incomplete_data)
    assert excinfo.value.args == ("pegType",)


def test_pull_stables_single_stablecoin():
    result = StableCoin.of(
        stablecoin_id="sample-stablecoin",
        data=sample_breakdown_data,
    )

    assert len(result.balances) == 2
    assert result.metadata == {
        "id": "sample-stablecoin",
        "name": "Sample Stablecoin",
        "address": "0xSampleAddress",
        "symbol": "SSC",
        "url": "https://samplestablecoin.com",
        "pegType": "peggedUSD",
        "pegMechanism": "fiat-backed",
        "description": "A sample stablecoin for testing.",
        "mintRedeemDescription": "Mint and redeem instructions.",
        "onCoinGecko": "true",
        "gecko_id": "sample-stablecoin",
        "cmcId": "12345",
        "priceSource": "coingecko",
        "twitter": "@samplestablecoin",
        "price": 1.0,
    }
    assert result.balances == [
        {
            "id": "sample-stablecoin",
            "chain": "Ethereum",
            "dt": "2024-11-06",
            "circulating": Decimal("1000000"),
            "bridged_to": Decimal("200000"),
            "minted": Decimal("1200000"),
            "unreleased": Decimal("0"),
            "name": "Sample Stablecoin",
            "symbol": "SSC",
        },
        {
            "id": "sample-stablecoin",
            "chain": "Ethereum",
            "dt": "2024-11-04",
            "circulating": Decimal("900000"),
            "bridged_to": Decimal("180000"),
            "minted": Decimal("1080000"),
            "unreleased": Decimal("0"),
            "name": "Sample Stablecoin",
            "symbol": "SSC",
        },
    ]


def test_pull_stables_another_stablecoin():
    result = StableCoin.of(
        stablecoin_id="another-stablecoin",
        data=another_sample_breakdown_data,
    )

    assert len(result.balances) == 1

    assert result.metadata == {
        "id": "another-stablecoin",
        "name": "Another Stablecoin",
        "address": "0xAnotherSampleAddress",
        "symbol": "ASC",
        "url": "https://anotherstablecoin.com",
        "pegType": "peggedUSD",
        "pegMechanism": "algorithmic",
        "description": "Another sample stablecoin for testing.",
        "mintRedeemDescription": "Mint and redeem instructions.",
        "onCoinGecko": "true",
        "gecko_id": "another-stablecoin",
        "cmcId": "67890",
        "priceSource": "coingecko",
        "twitter": "@anotherstablecoin",
        "price": 1.0,
    }
    assert result.balances == [
        {
            "id": "another-stablecoin",
            "chain": "Binance Smart Chain",
            "dt": "2024-11-06",
            "circulating": Decimal("167489338"),
            "bridged_to": Decimal("100000"),
            "minted": Decimal("600000"),
            "unreleased": Decimal("0"),
            "name": "Another Stablecoin",
            "symbol": "ASC",
        }
    ]


def test_process_breakdown_stables_empty_balances():
    # Test process_breakdown_stables with empty 'chainBalances' (should return empty DataFrame)
    data_with_empty_balances = sample_breakdown_data.copy()
    data_with_empty_balances["chainBalances"] = {}
    metadata, balances = single_stablecoin_balances(data_with_empty_balances)
    assert not balances
    assert metadata["id"] == "sample-stablecoin"


def test_process_breakdown_stables_missing_mandatory_metadata():
    # Test process_breakdown_stables with missing mandatory metadata fields (should raise KeyError)
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["name"]  # Remove a mandatory field
    with pytest.raises(KeyError) as excinfo:
        single_stablecoin_balances(incomplete_data)
    assert excinfo.value.args == ("name",)


def test_process_breakdown_stables_optional_metadata():
    # Test process_breakdown_stables with missing optional metadata fields
    incomplete_data = sample_breakdown_data.copy()
    del incomplete_data["description"]  # Remove an optional field
    metadata, balances = single_stablecoin_balances(incomplete_data)
    assert "description" in metadata
    assert metadata["description"] is None  # Should be None if missing
