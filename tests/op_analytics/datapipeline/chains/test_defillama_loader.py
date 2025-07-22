import polars as pl
from unittest.mock import patch
from op_analytics.datapipeline.chains.loaders.defillama_loader import DefiLlamaChainMetadataLoader

SAMPLE_DF = pl.DataFrame(
    {
        "chain_id": [1, 2],
        "chain_name": ["Ethereum", "Optimism"],
        "dfl_tracks_tvl": [1, 1],
        "is_evm": [1, 1],
        "is_superchain": [0, 1],
        "layer": ["L1", "L2"],
        "is_rollup": [0, 1],
        "gecko_id": ["ethereum", "optimism"],
        "cmc_id": ["1027", "11840"],
        "symbol": ["ETH", "OP"],
    }
)


class MockChainsMetadata:
    df = SAMPLE_DF
    chains = ["Ethereum", "Optimism"]


@patch(
    "op_analytics.datasources.defillama.chaintvl.metadata.ChainsMetadata.fetch",
    return_value=MockChainsMetadata,
)
def test_defillama_chain_metadata_loader_success(mock_fetch):
    loader = DefiLlamaChainMetadataLoader()
    df = loader.run()
    required = set(loader.REQUIRED_FIELDS)
    assert required.issubset(set(df.columns)), (
        f"Missing required columns: {required - set(df.columns)}"
    )
    assert df.shape[0] == 2
    assert (df["source_name"] == "defillama").all()
    assert (df["source_rank"] == 5).all()
    assert df["chain_name"].to_list() == ["Ethereum", "Optimism"]
    assert df["display_name"].to_list() == ["Ethereum", "Optimism"]
