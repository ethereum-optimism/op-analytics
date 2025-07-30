from dagster import AssetExecutionContext, Config, asset
from pydantic import Field
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date
from op_analytics.datapipeline.chains.aggregator import build_all_chains_metadata
from op_analytics.datapipeline.chains.datasets import ChainMetadata
from op_analytics.datapipeline.chains.ingestors import (
    ingest_with_deduplication,
    ingest_from_l2beat,
    ingest_from_defillama,
    ingest_from_dune,
    ingest_from_bq_op_stack,
    ingest_from_bq_goldsky,
)

log = structlog.get_logger()


class ChainMetadataConfig(Config):
    output_bq_table: str = Field(
        default="analytics.chain_metadata",
        description="Target BigQuery table name for aggregated metadata output",
    )
    bq_project_id: str = Field(description="BigQuery project ID for data operations")
    bq_dataset_id: str = Field(description="BigQuery dataset ID for table operations")
    process_date: str | None = Field(
        default=None, description="Date to process (YYYY-MM-DD format), defaults to today"
    )


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def l2beat_daily(context: AssetExecutionContext, config: ChainMetadataConfig) -> bool:
    """Fetch L2Beat chain metadata with daily partitioning and deduplication."""
    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()

    result = ingest_with_deduplication(
        source_name="L2Beat API",
        fetch_func=ingest_from_l2beat,
        dataset=ChainMetadata.L2BEAT,
        process_dt=process_dt,
    )
    context.log.info(f"L2Beat: {'updated' if result else 'skipped (no changes)'}")
    return result


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def defillama_daily(context: AssetExecutionContext, config: ChainMetadataConfig) -> bool:
    """Fetch DefiLlama chain metadata with daily partitioning and deduplication."""
    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()

    result = ingest_with_deduplication(
        source_name="DefiLlama API",
        fetch_func=ingest_from_defillama,
        dataset=ChainMetadata.DEFILLAMA,
        process_dt=process_dt,
    )
    context.log.info(f"DefiLlama: {'updated' if result else 'skipped (no changes)'}")
    return result


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def dune_daily(context: AssetExecutionContext, config: ChainMetadataConfig) -> bool:
    """Fetch Dune chain metadata with daily partitioning and deduplication."""
    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()

    result = ingest_with_deduplication(
        source_name="Dune Analytics",
        fetch_func=ingest_from_dune,
        dataset=ChainMetadata.DUNE,
        process_dt=process_dt,
    )
    context.log.info(f"Dune: {'updated' if result else 'skipped (no changes)'}")
    return result


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def bq_op_stack_daily(context: AssetExecutionContext, config: ChainMetadataConfig) -> bool:
    """Fetch BigQuery OP Stack chain metadata with daily partitioning and deduplication."""
    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()

    result = ingest_with_deduplication(
        source_name="BQ OP Stack",
        fetch_func=lambda: ingest_from_bq_op_stack(config.bq_project_id, config.bq_dataset_id),
        dataset=ChainMetadata.BQ_OP_STACK,
        process_dt=process_dt,
    )
    context.log.info(f"BQ OP Stack: {'updated' if result else 'skipped (no changes)'}")
    return result


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def bq_goldsky_daily(context: AssetExecutionContext, config: ChainMetadataConfig) -> bool:
    """Fetch BigQuery Goldsky chain metadata with daily partitioning and deduplication."""
    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()

    result = ingest_with_deduplication(
        source_name="BQ Goldsky",
        fetch_func=lambda: ingest_from_bq_goldsky(config.bq_project_id, config.bq_dataset_id),
        dataset=ChainMetadata.BQ_GOLDSKY,
        process_dt=process_dt,
    )
    context.log.info(f"BQ Goldsky: {'updated' if result else 'skipped (no changes)'}")
    return result


@asset(
    group_name="chain_metadata",
    compute_kind="python",
    tags={"dagster/k8s_node_selector": "ingestion-small"},
)
def aggregated_daily(
    context: AssetExecutionContext,
    config: ChainMetadataConfig,
    l2beat_daily: bool,
    defillama_daily: bool,
    dune_daily: bool,
    bq_op_stack_daily: bool,
    bq_goldsky_daily: bool,
) -> pl.DataFrame:
    """Aggregate all chain metadata sources into final dataset."""
    context.log.info("Starting daily chain metadata aggregation")

    updates = {
        "L2Beat": l2beat_daily,
        "DefiLlama": defillama_daily,
        "Dune": dune_daily,
        "BQ OP Stack": bq_op_stack_daily,
        "BQ Goldsky": bq_goldsky_daily,
    }

    updated = [name for name, status in updates.items() if status]
    skipped = [name for name, status in updates.items() if not status]

    context.log.info(f"Updated: {updated}, Skipped: {skipped}")

    result_df = build_all_chains_metadata(
        output_bq_table=config.output_bq_table,
        manual_mappings_filepath="resources/manual_chain_mappings.csv",
        bq_project_id=config.bq_project_id,
        bq_dataset_id=config.bq_dataset_id,
        csv_path="",
    )

    process_dt = date.fromisoformat(config.process_date) if config.process_date else now_date()
    df_with_date = result_df.with_columns(pl.lit(process_dt).alias("dt"))
    ChainMetadata.AGGREGATED.write(df_with_date, sort_by=["chain_key"])

    context.log.info(f"Aggregated {result_df.height} records to {config.output_bq_table}")
    return result_df


@asset
def all_chains_metadata_asset(
    context: AssetExecutionContext, config: ChainMetadataConfig
) -> pl.DataFrame:
    """Legacy asset that aggregates chain metadata from multiple sources."""
    result_df: pl.DataFrame = build_all_chains_metadata(
        output_bq_table=config.output_bq_table,
        manual_mappings_filepath="resources/manual_chain_mappings.csv",
        bq_project_id=config.bq_project_id,
        bq_dataset_id=config.bq_dataset_id,
        csv_path="",
    )

    context.log.info(f"Chain metadata aggregation completed: {result_df.height} records")
    return result_df
