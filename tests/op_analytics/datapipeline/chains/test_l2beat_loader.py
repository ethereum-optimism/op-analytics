import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.l2beat_loader import L2BeatChainMetadataLoader

SAMPLE_DF = pl.DataFrame(
    {
        "id": ["1", "2"],
        "name": ["Arbitrum", "Base"],
        "slug": ["arbitrum", "base"],
        "type": ["rollup", "rollup"],
        "category": ["Optimistic", "Optimistic"],
    }
)


class MockL2BeatProjectsSummary:
    summary_df = SAMPLE_DF
    projects = []


@patch(
    "op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch",
    return_value=MockL2BeatProjectsSummary,
)
def test_l2beat_chain_metadata_loader_success(mock_fetch):
    loader = L2BeatChainMetadataLoader()
    df = loader.run()
    required = set(loader.REQUIRED_FIELDS)
    assert required.issubset(set(df.columns)), (
        f"Missing required columns: {required - set(df.columns)}"
    )
    assert df.shape[0] == 2
    assert (df["source_name"] == "l2beat").all()
    assert (df["source_rank"] == 2).all()
    assert df["chain_name"].to_list() == ["Arbitrum", "Base"]
    assert df["display_name"].to_list() == ["Arbitrum", "Base"]
