"""
ChainMetadataAggregator for op_analytics.datapipeline.chains

Aggregates, deduplicates, and outputs harmonized chain metadata from all registered loaders.
"""

from typing import Optional

import polars as pl

from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import LoaderRegistry
from op_analytics.datapipeline.chains.mapping_utils import load_manual_mappings, apply_mapping_rules

log = structlog.get_logger()


def build_all_chains_metadata(
    output_bq_table: str,
    csv_path: str,
    manual_mappings_filepath: Optional[str] = None,
) -> pl.DataFrame:
    log.info("Starting chain metadata aggregation pipeline")

    loader_names = LoaderRegistry.list_loaders()
    dfs = []

    for loader_name in loader_names:
        log.info(f"Running loader: {loader_name}")
        loader_cls = LoaderRegistry.get_loader(loader_name)

        loader = loader_cls(csv_path=csv_path) if loader_name == "csv_loader" else loader_cls()
        df = loader.run()
        dfs.append(df)
        log.info(f"Loaded {len(df)} records from {loader_name}")

    all_chains_df = pl.concat(dfs, how="diagonal")
    log.info(f"Concatenated {len(all_chains_df)} total records")

    all_chains_df = all_chains_df.sort("source_rank").group_by("chain_key").first()
    log.info(f"Aggregated {len(all_chains_df)} unique chains")

    mapping_rules = load_manual_mappings(manual_mappings_filepath)
    all_chains_df = apply_mapping_rules(all_chains_df, mapping_rules)
    log.info("Manual mappings applied")

    dataset, table_name = output_bq_table.split(".", 1)
    log.info(f"Writing {len(all_chains_df)} records to BigQuery table: {output_bq_table}")
    overwrite_unpartitioned_table(all_chains_df, dataset, table_name)
    log.info("Successfully wrote data to BigQuery")

    return all_chains_df
