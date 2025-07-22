import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.bigquery.client import init_client
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from typing import Optional

log = structlog.get_logger()

BQ_CHAIN_METADATA_QUERY = """
SELECT
    mainnet_chain_id AS chain_id,
    chain_name,
    display_name,
    public_mainnet_launch_date
FROM `api_table_uploads.op_stack_chain_metadata`
"""


class BQChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from a BigQuery table (default: op_stack_chain_metadata).
    """

    def __init__(
        self, bq_project_id: Optional[str] = None, bq_dataset_id: Optional[str] = None, **kwargs
    ):
        super().__init__(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, **kwargs)
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info(
            "Querying chain metadata from BigQuery",
            project=self.bq_project_id,
        )
        client = init_client()
        query_job = client.query(BQ_CHAIN_METADATA_QUERY)
        pandas_df = query_job.to_dataframe()
        df = pl.from_pandas(pandas_df)
        df = df.with_columns(
            [
                pl.lit("op labs").alias("source_name"),
                pl.lit(1).alias("source_rank"),
            ]
        )
        return df


LoaderRegistry.register("bq_chain_metadata", BQChainMetadataLoader)


def load_bq_chain_metadata(
    bq_project_id: Optional[str] = None, bq_dataset_id: Optional[str] = None
) -> pl.DataFrame:
    """
    Load chain metadata from BigQuery using the loader.
    """
    loader = BQChainMetadataLoader(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id)
    return loader.run()
