import polars as pl
from unittest.mock import patch
from op_analytics.datasources.chainsmeta.bq_chain_metadata import BQChainMetadataLoader
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA


@patch("op_analytics.datasources.chainsmeta.bq_chain_metadata.pl.from_pandas")
def test_bq_chain_metadata_loader_success(mock_from_pandas):
    # Mock the DataFrame that pl.from_pandas will return
    mock_from_pandas.return_value = pl.DataFrame(
        {
            "chain_name": ["optimism", "base"],
            "chain_id": ["10", "8453"],
            "display_name": ["Optimism", "Base"],
            "public_mainnet_launch_date": ["2021-11-11", "2023-08-09"],
        }
    )

    # Run the loader
    loader = BQChainMetadataLoader(bq_project_id="test-project", bq_dataset_id="test-dataset")
    df = loader.run()

    # Assertions
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert df["chain_key"][0] == "optimism"
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())


@patch("op_analytics.datasources.chainsmeta.bq_chain_metadata.pl.from_pandas")
def test_bq_chain_metadata_loader_empty(mock_from_pandas):
    # Mock an empty DataFrame
    mock_from_pandas.return_value = pl.DataFrame(
        {
            "chain_name": [],
            "chain_id": [],
            "display_name": [],
            "public_mainnet_launch_date": [],
        }
    )

    loader = BQChainMetadataLoader(bq_project_id="test-project", bq_dataset_id="test-dataset")
    df = loader.run()

    assert df.height == 0
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())
