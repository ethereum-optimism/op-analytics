import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.dune_loader import DuneChainMetadataLoader

# Mock DataFrame with raw Dune columns as expected by the loader
SAMPLE_DF = pl.DataFrame(
    {
        "chain": ["Ethereum", "Optimism"],
    }
)


class MockDuneDexTradesSummary:
    df = SAMPLE_DF


def test_dune_chain_metadata_loader(monkeypatch):
    with patch(
        "op_analytics.datasources.dune.dextrades.DuneDexTradesSummary.fetch",
        return_value=MockDuneDexTradesSummary,
    ):
        loader = DuneChainMetadataLoader()
        df = loader.run()
        required = set(loader.REQUIRED_FIELDS)
        assert required.issubset(set(df.columns)), (
            f"Missing required columns: {required - set(df.columns)}"
        )
        assert (df["source_name"] == "dune").all()
        assert (df["source_rank"] == 4.5).all()
        assert df["chain_name"].to_list() == ["Ethereum", "Optimism"]
        assert df["display_name"].to_list() == ["Ethereum", "Optimism"]
