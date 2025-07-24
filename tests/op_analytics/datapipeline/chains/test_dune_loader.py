import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.dune_loader import DuneChainMetadataLoader
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA


@patch("op_analytics.datasources.dune.dextrades.DuneDexTradesSummary.fetch")
def test_dune_loader_success(mock_fetch):
    # Mock the API response
    mock_df = pl.DataFrame(
        {
            "blockchain": ["optimism", "base"],
            "dt": ["2023-01-01", "2023-01-01"],
        }
    )

    class MockApiResponse:
        df = mock_df

    mock_fetch.return_value = MockApiResponse()

    # Run the loader
    loader = DuneChainMetadataLoader()
    df = loader.run()

    # Sort by chain_key to ensure consistent order
    df = df.sort("chain_key")

    # Assertions
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert df["chain_key"][1] == "optimism"
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())


@patch("op_analytics.datasources.dune.dextrades.DuneDexTradesSummary.fetch")
def test_dune_loader_empty(mock_fetch):
    # Mock an empty API response
    class MockApiResponse:
        df = pl.DataFrame({"blockchain": []})  # Ensure column exists for logic

    mock_fetch.return_value = MockApiResponse()

    loader = DuneChainMetadataLoader()
    df = loader.run()

    assert df.height == 0
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())
