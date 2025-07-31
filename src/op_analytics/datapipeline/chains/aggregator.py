"""Chain metadata aggregator for op_analytics.datapipeline.chains"""

from datetime import date

import polars as pl

from op_analytics.coreutils.bigquery.write import overwrite_unpartitioned_table
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.time import now_date
from op_analytics.datapipeline.chains.datasets import ChainMetadata
from op_analytics.datapipeline.chains.mapping_utils import (
    apply_mapping_rules,
    load_manual_mappings,
)
from op_analytics.coreutils.partitioned.dailydatawrite import determine_location
from op_analytics.datapipeline.chains import ingestors

log = structlog.get_logger()


def _read_latest_from_gcs(process_dt: date) -> list[pl.DataFrame]:
    """Read latest data from all partitioned sources."""

    date_str = process_dt.strftime("%Y-%m-%d")
    location = determine_location()  # Use the same location as writes

    sources = [
        (ChainMetadata.L2BEAT, "L2Beat"),
        (ChainMetadata.DEFILLAMA, "DefiLlama"),
        (ChainMetadata.DUNE, "Dune"),
        (ChainMetadata.BQ_OP_STACK, "BQ OP Stack"),
        (ChainMetadata.BQ_GOLDSKY, "BQ Goldsky"),
    ]

    dataframes = []

    for dataset, name in sources:
        try:
            # Use only min_date for single date to avoid min_date=max_date issue
            df = dataset.read_polars(min_date=date_str, location=location)
            log.info(f"Read {df.height} records from {name} partition")
            dataframes.append(df)
        except Exception as e:
            log.error(f"Failed to read {name} from partitioned storage: {e}")
            continue

    return dataframes


def build_all_chains_metadata_from_gcs(
    output_bq_table: str,
    manual_mappings_filepath: str,
    process_dt: date | None = None,
) -> pl.DataFrame:
    """Orchestrates chain metadata aggregation pipeline from GCS storage."""
    process_dt = process_dt or now_date()

    dataframes = _read_latest_from_gcs(process_dt)

    if not dataframes:
        log.error(f"No data available from any source for {process_dt}")
        return pl.DataFrame()

    all_chains_df = pl.concat(dataframes, how="vertical")
    unique_df = all_chains_df.sort("source_rank").group_by("chain_key").first()
    log.info(f"Aggregated {unique_df.height} unique records from GCS sources")

    mapping_rules = load_manual_mappings(manual_mappings_filepath)
    final_df = apply_mapping_rules(unique_df, mapping_rules)

    dataset, table_name = output_bq_table.split(".", 1)
    overwrite_unpartitioned_table(final_df, dataset, table_name)
    log.info(f"Wrote {final_df.height} records to {output_bq_table}")

    return final_df


def build_all_chains_metadata(
    output_bq_table: str,
    manual_mappings_filepath: str,
    bq_project_id: str,
    bq_dataset_id: str,
    csv_path: str = "",  # Legacy parameter kept for compatibility
) -> pl.DataFrame:
    """Legacy function for backward compatibility."""
    log.warning(
        "Using legacy aggregator - consider migrating to build_all_chains_metadata_from_gcs()"
    )

    ingestor_configs = [
        ("L2Beat", ingestors.ingest_from_l2beat),
        ("DefiLlama", ingestors.ingest_from_defillama),
        ("Dune", ingestors.ingest_from_dune),
        ("BQ OP Stack", ingestors.ingest_from_bq_op_stack),
        ("BQ Goldsky", ingestors.ingest_from_bq_goldsky),
    ]

    dataframes = []
    for name, ingestor in ingestor_configs:
        df = ingestor()
        log.info(f"Ingested {df.height} records from {name}")
        dataframes.append(df)

    all_chains_df = pl.concat(dataframes, how="vertical")
    unique_df = all_chains_df.sort("source_rank").group_by("chain_key").first()
    log.info(f"Aggregated {unique_df.height} unique records")

    mapping_rules = load_manual_mappings(manual_mappings_filepath)
    final_df = apply_mapping_rules(unique_df, mapping_rules)

    dataset, table_name = output_bq_table.split(".", 1)
    overwrite_unpartitioned_table(final_df, dataset, table_name)
    log.info(f"Wrote {final_df.height} records to {output_bq_table}")

    return final_df
