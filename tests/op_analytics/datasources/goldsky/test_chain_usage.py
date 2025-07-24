import polars as pl
from unittest.mock import patch
from op_analytics.datasources.goldsky.chain_usage import GoldskyChainUsageLoader
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA


@patch("op_analytics.datasources.goldsky.chain_usage.pl.from_pandas")
def test_goldsky_loader_success(mock_from_pandas):
    # Mock the DataFrame that pl.from_pandas will return
    mock_from_pandas.return_value = pl.DataFrame(
        {
            "chain_name": ["optimism", "base"],
            "chain_id": ["10", "8453"],
            "dt": ["2023-01-01", "2023-01-01"],
            "num_raw_txs": [100, 200],
            "l2_gas_used": [1000, 2000],
            "l2_eth_fees_per_day": [0.1, 0.2],
        }
    )

    # Run the loader
    loader = GoldskyChainUsageLoader(bq_project_id="test-project", bq_dataset_id="test-dataset")
    df = loader.run()

    # Assertions
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "chain_key" in df.columns
    assert df["chain_key"][0] == "optimism"
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())


@patch("op_analytics.datasources.goldsky.chain_usage.pl.from_pandas")
def test_goldsky_loader_empty(mock_from_pandas):
    # Mock an empty DataFrame
    mock_from_pandas.return_value = pl.DataFrame(
        {
            "chain_name": [],
            "chain_id": [],
            "dt": [],
            "num_raw_txs": [],
            "l2_gas_used": [],
            "l2_eth_fees_per_day": [],
        }
    )

    loader = GoldskyChainUsageLoader(bq_project_id="test-project", bq_dataset_id="test-dataset")
    df = loader.run()

    assert df.height == 0
    assert list(df.columns) == list(CHAIN_METADATA_SCHEMA.keys())
