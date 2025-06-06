from dagster import AssetExecutionContext, Config, asset
from pydantic import Field

from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata

log = structlog.get_logger()


class ChainMetadataConfig(Config):
    output_bq_table: str = Field(
        description="Target BigQuery table name for aggregated metadata output"
    )
    manual_mappings_filepath: str = Field(description="Path to manual mappings configuration file")
    bq_project_id: str = Field(description="BigQuery project ID for data operations")
    bq_dataset_id: str = Field(description="BigQuery dataset ID for table operations")


@asset
def all_chains_metadata_asset(context: AssetExecutionContext, config: ChainMetadataConfig):
    log.info("Chain metadata aggregation asset started")

    log.info(
        "Asset configuration",
        output_bq_table=config.output_bq_table,
        manual_mappings_filepath=config.manual_mappings_filepath,
        bq_project_id=config.bq_project_id,
        bq_dataset_id=config.bq_dataset_id,
    )

    build_all_chains_metadata(
        output_bq_table=config.output_bq_table,
        manual_mappings_filepath=config.manual_mappings_filepath,
        bq_project_id=config.bq_project_id,
        bq_dataset_id=config.bq_dataset_id,
    )

    log.info("Chain metadata aggregation asset completed successfully")
