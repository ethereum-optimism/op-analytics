import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.dune_loader import DuneChainMetadataLoader

SAMPLE_DF = pl.DataFrame(
    {
        "chain": ["Arbitrum", "Base"],
        "dt": ["2024-01-01", "2024-01-02"],
    }
)


class MockDuneDexTradesSummary:
    df = SAMPLE_DF


@patch(
    "op_analytics.datasources.dune.dextrades.DuneDexTradesSummary.fetch",
    return_value=MockDuneDexTradesSummary,
)
def test_dune_chain_metadata_loader_success(mock_fetch):
    loader = DuneChainMetadataLoader()
    df = loader.run()
    required = set(loader.REQUIRED_FIELDS)
    assert required.issubset(set(df.columns)), (
        f"Missing required columns: {required - set(df.columns)}"
    )
    assert df.shape[0] == 2
    assert (df["source_name"] == "dune").all()
    assert (df["source_rank"] == 4.5).all()
    assert df["chain_name"].to_list() == ["Arbitrum", "Base"]
    assert df["display_name"].to_list() == ["Arbitrum", "Base"]
