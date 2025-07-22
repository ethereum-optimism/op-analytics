import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.defillama_loader import DefiLlamaChainMetadataLoader

# Mock DataFrame with raw DefiLlama columns as expected by the loader
SAMPLE_DF = pl.DataFrame(
    {
        "chain_id": [1, 2],
        "chain_name": ["Ethereum", "Optimism"],
    }
)


class MockChainsMetadata:
    df = SAMPLE_DF
    chains = ["Ethereum", "Optimism"]


def test_defillama_chain_metadata_loader(monkeypatch):
    with patch(
        "op_analytics.datasources.defillama.chaintvl.metadata.ChainsMetadata.fetch",
        return_value=MockChainsMetadata,
    ):
        loader = DefiLlamaChainMetadataLoader()
        df = loader.run()
        required = set(loader.REQUIRED_FIELDS)
        assert required.issubset(set(df.columns)), (
            f"Missing required columns: {required - set(df.columns)}"
        )
        assert (df["source_name"] == "defillama").all()
        assert (df["source_rank"] == 5).all()
        assert df["chain_id"].to_list() == [1, 2]
        assert df["chain_name"].to_list() == ["Ethereum", "Optimism"]
        assert df["display_name"].to_list() == ["Ethereum", "Optimism"]
