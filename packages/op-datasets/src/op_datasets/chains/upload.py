import pandas as pd
import polars as pl

from op_coreutils.gsheets import update_gsheet, record_changes

from .across_bridge import upload_across_bridge_addresses
from .goldsky_chains import goldsky_mainnet_chains_df
from .load import load_chain_metadata


GSHEET_NAME = "chain_metadata"

WORKSHEET_METADATA = "Chain Metadata"
WORKSHEET_GOLDSKY_CHAINS = "Goldsky Chains"


def upload_all():
    # Save the clean df to Google Sheets
    clean_df = load_chain_metadata()
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_METADATA,
        dataframe=to_pandas(clean_df),
    )

    goldsky_df = goldsky_mainnet_chains_df()
    # Save goldsky chains.
    update_gsheet(
        location_name=GSHEET_NAME,
        worksheet_name=WORKSHEET_GOLDSKY_CHAINS,
        dataframe=to_pandas(goldsky_df),
    )

    # Save a record of what was done.
    record_changes(
        GSHEET_NAME,
        messages=[
            f"Updated worksheeet: {_}" for _ in [WORKSHEET_METADATA, WORKSHEET_GOLDSKY_CHAINS]
        ],
    )

    # Upload the across bridge addresses.
    # Makes sure they are consistent with Chain Metadata.
    upload_across_bridge_addresses(chains_df=goldsky_df)


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
