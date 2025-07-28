import polars as pl
from op_analytics.coreutils.bigquery.client import init_client
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader

BQ_CHAIN_METADATA_QUERY = """
SELECT
    mainnet_chain_id AS chain_id,
    chain_name,
    display_name,
    public_mainnet_launch_date
FROM `api_table_uploads.op_stack_chain_metadata`
"""


class BQChainMetadataLoader(BaseChainMetadataLoader):
    def __init__(self, bq_project_id=None, bq_dataset_id=None, **kwargs):
        super().__init__(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, **kwargs)

    def load_data(self) -> pl.DataFrame:
        client = init_client()
        query_job = client.query(BQ_CHAIN_METADATA_QUERY)
        df = pl.from_pandas(query_job.to_dataframe())
        return self.add_metadata_columns(
            df=df, chain_key_col="chain_name", source="op labs", source_rank=1
        )


register_loader("bq_chain_metadata", BQChainMetadataLoader)
