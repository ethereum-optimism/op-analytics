"""
Functional ingestors for the chain metadata pipeline.

Each function fetches data from a source and returns a standardized Polars DataFrame.
"""

import polars as pl

from op_analytics.coreutils.bigquery.client import init_client
from op_analytics.datapipeline.chains.schemas import (
    CHAIN_METADATA_SCHEMA,
    DEFAULT_VALUES,
    generate_chain_key,
)
from op_analytics.datasources.defillama.chaintvl.metadata import ChainsMetadata
from op_analytics.datasources.dune.dextrades import DuneDexTradesSummary
from op_analytics.datasources.l2beat.projects import L2BeatProjectsSummary

# ----------------------------------------
# Constants
# ----------------------------------------

OP_STACK_QUERY: str = "SELECT mainnet_chain_id, chain_name, display_name, public_mainnet_launch_date FROM `api_table_uploads.op_stack_chain_metadata`"
GOLDSKY_QUERY: str = "SELECT dt, chain_id, chain_name, num_raw_txs, l2_gas_used, l2_eth_fees_per_day FROM `api_table_uploads.daily_aggegate_l2_chain_usage_goldsky`"


# ----------------------------------------
# Helper Functions
# ----------------------------------------


def _process_df(
    df: pl.DataFrame,
    chain_key_col: str,
    source: str,
    rank: int,
    renames: dict[str, str] = {},
) -> pl.DataFrame:
    """Common processing: validate → add metadata → rename → finalize."""
    if df.height == 0:
        raise ValueError(f"Empty DataFrame from {source}")

    df = df.with_columns(
        generate_chain_key(chain_key_col),
        pl.lit(source).alias("source_name"),
        pl.lit(rank).alias("source_rank"),
    )

    df = df.rename(renames)

    # Add missing columns and cast to schema
    df = df.with_columns(
        *[
            pl.lit(DEFAULT_VALUES.get(col), dtype=dtype).alias(col)
            for col, dtype in CHAIN_METADATA_SCHEMA.items()
            if col not in df.columns
        ]
    ).with_columns(
        pl.when(pl.col("chain").is_null())
        .then(pl.col("chain_key"))
        .otherwise(pl.col("chain"))
        .alias("chain")
    )

    return df.select(
        [pl.col(col).cast(dtype, strict=False) for col, dtype in CHAIN_METADATA_SCHEMA.items()]
    )


# ----------------------------------------
# Ingestion Functions
# ----------------------------------------


def ingest_from_csv(csv_path: str) -> pl.DataFrame:
    """Ingests chain metadata from a local CSV file."""
    return _process_df(
        df=pl.read_csv(csv_path),
        chain_key_col="chain_name",
        source="csv",
        rank=1,
        renames={},
    )


def ingest_from_l2beat() -> pl.DataFrame:
    """Ingests chain metadata from the L2Beat API."""
    df: pl.DataFrame = L2BeatProjectsSummary.fetch().summary_df
    df = _process_df(
        df=df,
        chain_key_col="id",
        source="l2beat",
        rank=2,
        renames={
            "id": "chain_key",
            "name": "display_name",
            "stage": "l2b_stage",
            "da_badge": "l2b_da_layer",
            "category": "provider_entity",
            "vm_badge": "provider",
        },
    )

    if "isArchived" in df.columns:
        df = df.with_columns((~pl.col("isArchived")).alias("is_current_chain"))

    return df


def ingest_from_defillama() -> pl.DataFrame:
    """Ingests chain metadata from the DefiLlama API."""
    return _process_df(
        df=ChainsMetadata.fetch().df,
        chain_key_col="chain_name",
        source="defillama",
        rank=3,
        renames={"chain_name": "display_name", "symbol": "gas_token"},
    )


def ingest_from_dune() -> pl.DataFrame:
    """Ingests chain metadata from a Dune query."""
    df: pl.DataFrame = DuneDexTradesSummary.fetch().df
    chain_col: str = "blockchain" if "blockchain" in df.columns else "chain_name"

    return _process_df(
        df=df,
        chain_key_col=chain_col,
        source="dune",
        rank=4,
        renames={chain_col: "display_name", "project": "provider", "version": "provider_entity"},
    )


def ingest_from_bq_op_stack(project_id: str, dataset_id: str) -> pl.DataFrame:
    """Ingests OP Stack metadata from BigQuery."""
    client = init_client()
    df: pl.DataFrame = pl.from_pandas(client.query(OP_STACK_QUERY).to_dataframe())

    return _process_df(
        df=df,
        chain_key_col="chain_name",
        source="op labs",
        rank=1,
        renames={"mainnet_chain_id": "chain_id", "public_mainnet_launch_date": "op_governed_start"},
    )


def ingest_from_bq_goldsky(project_id: str, dataset_id: str) -> pl.DataFrame:
    """Ingests chain usage data from Goldsky via BigQuery."""
    client = init_client()
    df: pl.DataFrame = pl.from_pandas(client.query(GOLDSKY_QUERY).to_dataframe())
    df = df.with_columns(pl.col("chain_name").alias("display_name"))

    return _process_df(
        df=df,
        chain_key_col="chain_name",
        source="goldsky",
        rank=5,
        renames={},
    )
