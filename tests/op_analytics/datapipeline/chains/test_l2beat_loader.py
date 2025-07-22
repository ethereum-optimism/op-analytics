import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.l2beat_loader import L2BeatChainMetadataLoader

SAMPLE_DF = pl.DataFrame(
    {
        "chain_id": ["optimism", "base"],
        "chain_name": ["Optimism", "Base"],
        "display_name": ["Optimism", "Base"],
        "source_name": ["l2beat", "l2beat"],
        "source_rank": [2, 2],
    }
)


class MockL2BeatProjectsSummary:
    summary_df = SAMPLE_DF
    projects: list[str] = ["optimism", "base"]


def test_l2beat_chain_metadata_loader(monkeypatch):
    with patch(
        "op_analytics.datasources.l2beat.projects.L2BeatProjectsSummary.fetch",
        return_value=MockL2BeatProjectsSummary,
    ):
        loader = L2BeatChainMetadataLoader()
        df = loader.run()
        required = set(loader.REQUIRED_FIELDS)
        assert required.issubset(set(df.columns)), (
            f"Missing required columns: {required - set(df.columns)}"
        )
        assert (df["source_name"] == "l2beat").all()
        assert (df["source_rank"] == 2).all()
        assert df["chain_id"].to_list() == ["optimism", "base"]
        assert df["chain_name"].to_list() == ["Optimism", "Base"]
        assert df["display_name"].to_list() == ["Optimism", "Base"]
