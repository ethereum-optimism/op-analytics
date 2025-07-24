import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.l2beat_loader import L2BeatChainMetadataLoader


@patch("op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch")
def test_l2beat_loader_success(mock_fetch):
    # Mock the API response
    mock_df = pl.DataFrame(
        {
            "name": ["Optimism", "Base"],
            "id": ["optimism", "base"],
            "category": ["Optimistic Rollup", "Optimistic Rollup"],
            "stage": ["Stage 1", "Stage 0"],
            "provider": ["OP Stack", "OP Stack"],
            "providers": [["OP Stack"], ["OP Stack"]],
            "da_badge": ["DA Layer", "DA Layer"],
            "isUpcoming": [False, False],
            "isArchived": [False, False],
        }
    )

    class MockApiResponse:
        summary_df = mock_df

    mock_fetch.return_value = MockApiResponse()

    # Run the loader
    loader = L2BeatChainMetadataLoader()
    df = loader.run()

    # Test core loader functionality
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert "source_rank" in df.columns
    assert df["chain_key"][0] == "optimism"
    assert df["source_rank"][0] == 2


@patch("op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch")
def test_l2beat_loader_empty(mock_fetch):
    # Mock empty response
    mock_df = pl.DataFrame()

    class MockApiResponse:
        summary_df = mock_df

    mock_fetch.return_value = MockApiResponse()

    # Run the loader
    loader = L2BeatChainMetadataLoader()
    df = loader.run()

    # Should return empty DataFrame
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
