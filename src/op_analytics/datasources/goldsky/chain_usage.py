import polars as pl
from op_analytics.coreutils.bigquery.client import init_client
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader

GOLDSKY_QUERY = """
SELECT
    dt,
    chain_id,
    chain_name,
    chain_name AS display_name,
    num_raw_txs,
    l2_gas_used,
    l2_eth_fees_per_day
FROM `api_table_uploads.daily_aggegate_l2_chain_usage_goldsky`
"""


class GoldskyChainUsageLoader(BaseChainMetadataLoader):
    def __init__(self, bq_project_id=None, bq_dataset_id=None, **kwargs):
        super().__init__(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, **kwargs)

    def load_data(self) -> pl.DataFrame:
        client = init_client()
        query_job = client.query(GOLDSKY_QUERY)
        df = pl.from_pandas(query_job.to_dataframe())
        return self.add_metadata_columns(
            df=df, chain_key_col="chain_name", source="goldsky", source_rank=5
        )


register_loader("goldsky", GoldskyChainUsageLoader)
