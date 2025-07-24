import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.defillama_loader import DefiLlamaChainMetadataLoader


@patch("op_analytics.datasources.defillama.chaintvl.metadata.ChainsMetadata.fetch")
def test_defillama_loader_success(mock_fetch):
    # Mock the API response
    mock_df = pl.DataFrame(
        {
            "chain_name": ["Optimism", "Base"],
            "chain_id": [10, 8453],
            "gecko_id": ["optimism", "base"],
            "layer": ["L2", "L2"],
            "is_superchain": [True, True],
        }
    )

    class MockApiResponse:
        df = mock_df

    mock_fetch.return_value = MockApiResponse()

    # Run the loader
    loader = DefiLlamaChainMetadataLoader()
    df = loader.run()

    # Test core loader functionality
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert "source_rank" in df.columns
    assert df["chain_key"][0] == "optimism"
    assert df["source_rank"][0] == 3


@patch("op_analytics.datasources.defillama.chaintvl.metadata.ChainsMetadata.fetch")
def test_defillama_loader_empty(mock_fetch):
    # Mock empty response
    mock_df = pl.DataFrame()

    class MockApiResponse:
        df = mock_df

    mock_fetch.return_value = MockApiResponse()

    # Run the loader
    loader = DefiLlamaChainMetadataLoader()
    df = loader.run()

    # Should return empty DataFrame
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
