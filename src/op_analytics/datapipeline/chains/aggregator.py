"""
ChainMetadataAggregator for op_analytics.datapipeline.chains

Aggregates, deduplicates, and outputs harmonized chain metadata from all registered loaders.
"""

import polars as pl

from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import LoaderRegistry
from op_analytics.datapipeline.chains.mapping_utils import load_manual_mappings, apply_mapping_rules

log = structlog.get_logger()


def build_all_chains_metadata(
    output_bq_table: str,
    manual_mappings_filepath: str,
    bq_project_id: str,
    bq_dataset_id: str,
    csv_path: str,
) -> pl.DataFrame:
    """
    Orchestrates the complete chain metadata aggregation pipeline.

    Args:
        output_bq_table: Target BigQuery table name
        manual_mappings_filepath: Path to manual mappings configuration file
        bq_project_id: BigQuery project ID for data operations
        bq_dataset_id: BigQuery dataset ID for table operations
        csv_path: Path to CSV file for CSV loader

    Returns:
        Aggregated chain metadata DataFrame
    """
    loader_names = LoaderRegistry.list_loaders()
    log.info(f"Running {len(loader_names)} loaders: {loader_names}")

    # Load data from all loaders
    dfs = []
    for loader_name in loader_names:
        loader_cls = LoaderRegistry.get_loader(loader_name)
        if loader_cls is None:
            log.warning(f"Loader {loader_name} not found, skipping")
            continue

        # Simple loader instantiation based on known types
        if loader_name == "csv_loader":
            loader = loader_cls(csv_path=csv_path)
        elif loader_name in ["bq_chain_metadata", "goldsky"]:
            loader = loader_cls(bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id)
        else:
            loader = loader_cls()

        df = loader.run()
        dfs.append(df)
        log.info(f"Loaded {len(df)} records from {loader_name}")

    # Concatenate and deduplicate
    all_chains_df = pl.concat(dfs, how="vertical_relaxed")
    all_chains_df = all_chains_df.unique(subset=["chain_key", "source_name"], keep="first")
    log.info(f"Aggregated {len(all_chains_df)} unique chain records")

    # Apply manual mappings
    mapping_rules = load_manual_mappings(manual_mappings_filepath)
    all_chains_df = apply_mapping_rules(all_chains_df, mapping_rules)

    # Write to BigQuery
    dataset, table_name = output_bq_table.split(".", 1)
    overwrite_unpartitioned_table(all_chains_df, dataset, table_name)
    log.info(f"Wrote {len(all_chains_df)} records to {output_bq_table}")

    return all_chains_df
