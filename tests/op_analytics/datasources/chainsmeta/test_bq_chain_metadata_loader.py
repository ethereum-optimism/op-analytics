import pandas as pd
from op_analytics.datasources.chainsmeta.bq_chain_metadata import BQChainMetadataLoader
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
                "chain_id": [10, 11],
                "chain_name": ["OP Mainnet", "Base"],
                "display_name": ["OP Mainnet", "Base"],
                "public_mainnet_launch_date": ["2021-11-11", "2023-07-13"],
            }
        )
        return MockQueryJob(pandas_df)


def test_bq_chain_metadata_loader(monkeypatch):
    bigquery_client._CLIENT = None
    mock_client = MockClient()
    monkeypatch.setattr(
        "op_analytics.coreutils.bigquery.client._CLIENT",
        mock_client,
    )

    loader = BQChainMetadataLoader(bq_project_id="test-project")
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
    assert df["public_mainnet_launch_date"].to_list() == ["2021-11-11", "2023-07-13"]
