import pandas as pd
import polars as pl

from op_analytics.coreutils.gsheets import record_changes, update_gsheet
from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT

from .across_bridge import load_across_bridge_addresses, upload_across_bridge_addresses
from .goldsky_chains import goldsky_mainnet_chains_df
from .load import load_chain_metadata

GSHEET_NAME = "chain_metadata"

WORKSHEET_METADATA = "Chain Metadata"
WORKSHEET_GOLDSKY_CHAINS = "Goldsky Chains"


def upload_all():
    work_done = []

    # Save the clean df to Google Sheets
    clean_df = load_chain_metadata()
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_METADATA,
        dataframe=to_pandas(clean_df),
    )
    work_done.append("Updated worksheeet: {WORKSHEET_METADATA}")

    goldsky_df = goldsky_mainnet_chains_df()

    # Save goldsky chains.
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_GOLDSKY_CHAINS,
        dataframe=to_pandas(goldsky_df),
    )
    work_done.append("Updated worksheeet: {WORKSHEET_GOLDSKY_CHAINS}")

    # Upload clean_df to GCS.
    from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta

    ChainsMeta.CHAIN_METADATA_GSHEET.write(clean_df.with_columns(dt=pl.lit(DEFAULT_DT)))
    work_done.append(f"Uploaded to GCS: {ChainsMeta.CHAIN_METADATA_GSHEET.table}")

    # Save a record of what was done.
    record_changes(GSHEET_NAME, messages=work_done)

    # Load across_bridge
    # (verifies that the chain names are consistent with the Chain Metadata source of truth).
    across_bridge_df = load_across_bridge_addresses(chains_df=goldsky_df)

    # Upload the across bridge addresses.
    # Makes sure they are consistent with Chain Metadata.
    upload_across_bridge_addresses(across_bridge_df=across_bridge_df)

    # Store across_bridge_df as DailyData on GCS.
    ChainsMeta.ACROSS_BRIDGE_GSHEET.write(across_bridge_df.with_columns(dt=pl.lit(DEFAULT_DT)))


def to_pandas(clean_df: pl.DataFrame) -> pd.DataFrame:
    """Special handling of some columns with null values."""
    new_cols = {}
    if "mainnet_chain_id" in clean_df.columns:
        new_cols["mainnet_chain_id"] = (
            pl.when(pl.col("mainnet_chain_id").is_null())
            .then(pl.lit("NA"))
            .otherwise(pl.col("mainnet_chain_id").cast(pl.String))
        )

    if "block_time_sec" in clean_df.columns:
        new_cols["block_time_sec"] = (
            pl.when(pl.col("block_time_sec").is_null())
            .then(pl.lit("NA"))
            .otherwise(pl.col("block_time_sec").cast(pl.String))
        )

    return clean_df.with_columns(**new_cols).to_pandas()
