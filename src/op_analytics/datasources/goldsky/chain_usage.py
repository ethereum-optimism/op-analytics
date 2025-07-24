import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.bigquery.client import init_client
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema
from typing import Optional

log = structlog.get_logger()

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
    """
    Loads Goldsky chain usage data from BigQuery.
    """

    def __init__(
        self, bq_project_id: Optional[str] = None, bq_dataset_id: Optional[str] = None, **kwargs
    ):
        super().__init__(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, **kwargs)
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info(
            "Querying Goldsky chain usage data from BigQuery",
            project=self.bq_project_id,
        )
        client = init_client()
        query_job = client.query(GOLDSKY_QUERY)
        pandas_df = query_job.to_dataframe()
        df = pl.from_pandas(pandas_df)

        if df.height == 0:
            log.warning("BigQuery returned no data for Goldsky chain usage.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        df = df.with_columns(
            [
                pl.col("chain_name").alias("chain"),
                pl.col("chain_name")
                .str.to_lowercase()
                .str.replace_all(" ", "-")
                .alias("chain_key"),
                pl.lit("op labs").alias("source_name"),
                pl.lit(1).cast(pl.Int32).alias("source_rank"),
            ]
        )
        return harmonize_to_canonical_schema(df)


LoaderRegistry.register("goldsky", GoldskyChainUsageLoader)


def load_goldsky_chain_usage_data(
    bq_project_id: Optional[str] = None, bq_dataset_id: Optional[str] = None
) -> pl.DataFrame:
    """
    Convenience function to load Goldsky chain usage data.
    """
    loader = GoldskyChainUsageLoader(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id)
    return loader.run()
