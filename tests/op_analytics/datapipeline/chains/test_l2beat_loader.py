import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.l2beat_loader import L2BeatChainMetadataLoader
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA


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

    # Assertions
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert df["chain_key"][0] == "optimism"
    assert df["l2b_stage"][0] == "Stage 1"
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())


@patch("op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch")
def test_l2beat_loader_empty(mock_fetch):
    # Mock an empty API response
    class MockApiResponse:
        summary_df = pl.DataFrame()

    mock_fetch.return_value = MockApiResponse()

    loader = L2BeatChainMetadataLoader()
    df = loader.run()

    assert df.height == 0
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())
