"""
ChainMetadataAggregator for op_analytics.datapipeline.chains

Aggregates, deduplicates, and outputs harmonized chain metadata from all registered loaders.
"""

import polars as pl
import structlog

from op_analytics.datapipeline.chains.loaders.base import LoaderRegistry
from op_analytics.datapipeline.chains.schemas import CHAIN_METADATA_SCHEMA


log = structlog.get_logger()


def build_all_chains_metadata(
    output_bq_table: str | None = None,
    manual_mappings_filepath: str | None = None,
    bq_project_id: str | None = None,
    bq_dataset_id: str | None = None,
    csv_path: str | None = None,
    exclude_loaders: list[str] | None = None,
) -> pl.DataFrame:
    """
    Runs all registered chain metadata loaders, concatenates the results,
    and returns a unified Polars DataFrame.
    """
    log.info("Chain metadata aggregation pipeline started")
    if exclude_loaders is None:
        exclude_loaders = []

    # Initialize loaders
    loader_names = LoaderRegistry.list_loaders()
    loaders = []
    for name in loader_names:
        if name in exclude_loaders:
            log.info(f"Skipping loader: {name}")
            continue

        loader_class = LoaderRegistry.get_loader(name)
        if not loader_class:
            log.warning(f"Loader class not found for name: {name}")
            continue

        # Skip CSV loader if no path is provided
        if name == "csv" and not csv_path:
            continue

        config = {}
        if name in ["bq_chain_metadata", "goldsky"]:
            config["bq_project_id"] = bq_project_id
            config["bq_dataset_id"] = bq_dataset_id
        elif name == "csv" and csv_path:
            config["csv_path"] = csv_path

        # Pass only relevant configs
        import inspect

        sig = inspect.signature(loader_class.__init__)
        loader_config = {k: v for k, v in config.items() if k in sig.parameters}

        loaders.append(loader_class(**loader_config))

    dfs = [loader.run() for loader in loaders]
    if not dfs:
        return pl.DataFrame(schema=CHAIN_METADATA_SCHEMA)

    # Concatenate all DataFrames
    all_chains_df = pl.concat(dfs, how="diagonal")

    # Deduplicate based on chain_key, keeping the highest ranked source
    all_chains_df = all_chains_df.sort("source_rank").group_by("chain_key").first()

    log.info(f"Aggregated {len(all_chains_df)} unique chains")

    # # Apply manual mappings if provided
    # if manual_mappings_filepath:
    #     manual_mappings = pl.read_csv(manual_mappings_filepath)
    #     all_chains_df = all_chains_df.join(
    #         manual_mappings, on="chain_name", how="left"
    #     )
    #     # Coalesce columns to apply manual overrides
    #     for col in manual_mappings.columns:
    #         if col != "chain_name" and f"{col}_right" in all_chains_df.columns:
    #             all_chains_df = all_chains_df.with_column(
    #                 pl.coalesce(pl.col(f"{col}_right"), pl.col(col)).alias(col)
    #             )
    #             all_chains_df = all_chains_df.drop(f"{col}_right")

    # Write to BigQuery if an output table is specified
    if output_bq_table and bq_project_id and bq_dataset_id:
        from op_analytics.coreutils.gcp.bigquery import upload_df_to_bq

        log.info(f"Uploading to BigQuery table: {output_bq_table}")
        upload_df_to_bq(
            df=all_chains_df,
            project_id=bq_project_id,
            dataset_id=bq_dataset_id,
            table_name=output_bq_table,
            write_disposition="WRITE_TRUNCATE",
        )

    return all_chains_df
