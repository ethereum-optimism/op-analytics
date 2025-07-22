import pandas as pd
from op_analytics.datasources.goldsky.chain_usage import GoldskyChainUsageLoader
import op_analytics.coreutils.bigquery.client as bigquery_client


class MockQueryJob:
    def __init__(self, pandas_df):
        self.pandas_df = pandas_df

    def to_dataframe(self):
        return self.pandas_df


class MockClient:
    def query(self, query_string):
        pandas_df = pd.DataFrame(
            {
                "dt": ["2024-01-01", "2024-01-02"],
                "chain_id": [10, 11],
                "chain_name": ["OP Mainnet", "Base"],
                "display_name": ["OP Mainnet", "Base"],
                "num_raw_txs": [1000, 2000],
                "l2_gas_used": [500000, 800000],
                "l2_eth_fees_per_day": [123.45, 678.90],
            }
        )
        return MockQueryJob(pandas_df)


def test_goldsky_chain_usage_loader(monkeypatch):
    bigquery_client._CLIENT = None
    mock_client = MockClient()
    monkeypatch.setattr(
        "op_analytics.coreutils.bigquery.client._CLIENT",
        mock_client,
    )

    loader = GoldskyChainUsageLoader(bq_project_id="test-project")
    df = loader.run()
    required = set(loader.REQUIRED_FIELDS)
    assert required.issubset(set(df.columns)), (
        f"Missing required columns: {required - set(df.columns)}"
    )
    assert (df["source_name"] == "op labs").all()
    assert (df["source_rank"] == 1).all()
    assert df["chain_id"].to_list() == [10, 11]
    assert df["chain_name"].to_list() == ["OP Mainnet", "Base"]
    assert df["display_name"].to_list() == ["OP Mainnet", "Base"]
    assert df["dt"].to_list() == ["2024-01-01", "2024-01-02"]
    assert df["num_raw_txs"].to_list() == [1000, 2000]
    assert df["l2_gas_used"].to_list() == [500000, 800000]
    assert df["l2_eth_fees_per_day"].to_list() == [123.45, 678.90]
