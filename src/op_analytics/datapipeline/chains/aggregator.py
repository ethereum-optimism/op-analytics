"""
ChainMetadataAggregator for op_analytics.datapipeline.chains

This module orchestrates the aggregation of chain metadata from various sources.
"""

from typing import Any, Callable

import polars as pl

from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains import ingestors
from op_analytics.datapipeline.chains.mapping_utils import (
    apply_mapping_rules,
    load_manual_mappings,
)

log = structlog.get_logger()


def _run_ingestors(bq_project_id: str, bq_dataset_id: str) -> list[pl.DataFrame]:
    """Run all ingestors and return successful results."""
    ingestor_configs: list[tuple[str, Callable[[], pl.DataFrame]]] = [
        ("L2Beat", ingestors.ingest_from_l2beat),
        ("DefiLlama", ingestors.ingest_from_defillama),
        ("Dune", ingestors.ingest_from_dune),
        ("BQ OP Stack", lambda: ingestors.ingest_from_bq_op_stack(bq_project_id, bq_dataset_id)),
        ("BQ Goldsky", lambda: ingestors.ingest_from_bq_goldsky(bq_project_id, bq_dataset_id)),
    ]

    dataframes: list[pl.DataFrame] = []
    for name, ingestor in ingestor_configs:
        df: pl.DataFrame = ingestor()
        log.info(f"Ingested {df.height} records from {name}")
        dataframes.append(df)

    return dataframes


def build_all_chains_metadata(
    output_bq_table: str,
    manual_mappings_filepath: str,
    bq_project_id: str,
    bq_dataset_id: str,
    csv_path: str = "",  # Kept for backward compatibility but not used
) -> pl.DataFrame:
    """
    Orchestrates the complete chain metadata aggregation pipeline.

    Pipeline: ingest → concat → dedupe → map → write
    """
    # Ingest from all sources (CSV removed as it's redundant)
    dataframes: list[pl.DataFrame] = _run_ingestors(bq_project_id, bq_dataset_id)

    # Combine and deduplicate based on source rank (highest rank wins)
    all_chains_df: pl.DataFrame = pl.concat(dataframes, how="vertical")
    unique_df: pl.DataFrame = all_chains_df.sort("source_rank").group_by("chain_key").first()
    log.info(f"Aggregated {unique_df.height} unique records")

    # Apply manual mappings
    mapping_rules: list[dict[str, Any]] = load_manual_mappings(manual_mappings_filepath)
    final_df: pl.DataFrame = apply_mapping_rules(unique_df, mapping_rules)

    # Write to BigQuery
    dataset: str
    table_name: str
    dataset, table_name = output_bq_table.split(".", 1)
    overwrite_unpartitioned_table(final_df, dataset, table_name)
    log.info(f"Wrote {final_df.height} records to {output_bq_table}")

    return final_df
