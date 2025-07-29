from dagster import AssetExecutionContext, Config, asset
from pydantic import Field

import polars as pl

from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata


class ChainMetadataConfig(Config):
    output_bq_table: str = Field(
        description="Target BigQuery table name for aggregated metadata output"
    )
    manual_mappings_filepath: str = Field(description="Path to manual mappings configuration file")
    bq_project_id: str = Field(description="BigQuery project ID for data operations")
    bq_dataset_id: str = Field(description="BigQuery dataset ID for table operations")
    csv_path: str = Field(
        default="src/op_analytics/datapipeline/chains/resources/manual_chain_mappings.csv",
        description="Path to the chain metadata CSV file (base chain data)",
    )


@asset
def all_chains_metadata_asset(
    context: AssetExecutionContext, config: ChainMetadataConfig
) -> pl.DataFrame:
    """Asset that aggregates chain metadata from multiple sources."""
    result_df: pl.DataFrame = build_all_chains_metadata(
        output_bq_table=config.output_bq_table,
        manual_mappings_filepath=config.manual_mappings_filepath,
        bq_project_id=config.bq_project_id,
        bq_dataset_id=config.bq_dataset_id,
        csv_path=config.csv_path,
    )

    context.log.info(f"Chain metadata aggregation completed: {result_df.height} records")
    return result_df
