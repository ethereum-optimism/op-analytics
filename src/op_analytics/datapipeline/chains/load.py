from functools import cache

import polars as pl
from polars import datatypes

from op_analytics.coreutils.gsheets import read_gsheet
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path
from op_analytics.coreutils.testutils.dataframe import compare_dataframes

log = structlog.get_logger()


OP_CHAIN = "OP Chain"
OP_FORK = "OP Stack fork"


DEFAULT_CHAIN_METADATA_LOCATION = repo_path("op_chains_tracking/inputs/chain_metadata_raw.csv")

RAW_CHAIN_METADATA_SCHEMA = pl.Schema(
    {
        "chain_name": pl.String,
        "display_name": pl.String,
        "mainnet_chain_id": pl.Int64,
        "public_mainnet_launch_date": pl.String,
        "op_based_version": pl.String,
        "chain_type": pl.String,
        "op_chain_start": pl.String,
        "migration_start": pl.String,
        "has_mods": pl.String,
        "raas_deployer": pl.String,
        "rpc_url": pl.String,
        "product_website": pl.String,
        "block_explorer_url": pl.String,
        "github_url": pl.String,
        "defillama_slug": pl.String,
        "l2beat_slug": pl.String,
        "growthepie_origin_key": pl.String,
        "gas_token": pl.String,
        "cgt_coingecko_api_key": pl.String,
        "chain_layer": pl.String,
        "block_time_sec": pl.Float64,
        "da_layer": pl.String,
        "output_root_layer": pl.String,
        "system_config_proxy": pl.String,
        "oplabs_db_schema": pl.String,
        "goldsky_schema": pl.String,
        "dune_schema": pl.String,
        "flipside_schema": pl.String,
        "oso_schema": pl.String,
        "batchinbox_from": pl.String,
        "batchinbox_to": pl.String,
        "outputoracle_from": pl.String,
        "outputoracle_to_proxy": pl.String,
        "l1_standard_bridge": pl.String,
        "optimism_portal": pl.String,
        "dispute_game_factory": pl.String,
    }
)


@cache
def load_chain_metadata() -> pl.DataFrame:
    """Cached version of load chain metadata."""
    return load_chain_metadata_impl()


def load_chain_metadata_impl() -> pl.DataFrame:
    """Uncached version of load chain metadata.

    For use in unittests wehre read_gsheet is mocked and we don't want
    the cache in load_chain_metadata() to be updated.
    """
    # Read CSV from Google Sheets Input
    raw_records = read_gsheet(
        location_name="chain_metadata",
        worksheet_name="Chain Metadata [RAW INPUT]",
    )
    clean_df_gsheets = _clean(pl.DataFrame(raw_records, infer_schema_length=len(raw_records)))

    # Read CSV from the repo
    path = DEFAULT_CHAIN_METADATA_LOCATION

    if path is not None:
        with open(path, "r") as fcsv:
            raw_df = pl.read_csv(fcsv, schema_overrides=RAW_CHAIN_METADATA_SCHEMA)
            log.info(f"Loaded OP chains metadata from {path}")
            clean_df = _clean(raw_df)
        try:
            # Before comparing drop the columns that have been added to GSheets but are not
            # in chain_metadata_raw.csv.
            if "oplabs_testnet_db_schema" in clean_df_gsheets.columns:
                compare_df = clean_df_gsheets.drop("oplabs_testnet_db_schema")
            else:
                compare_df = clean_df_gsheets

            compare_dataframes(clean_df, compare_df)
        except AssertionError:
            log.info("[REPO vs. GSHEETS] ERROR: Chain Metadata is different")
        else:
            log.info("[REPO vs. GSHEETS] OK: Chain Metadata is equal")

    return clean_df_gsheets


def _clean(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Clean and enrich the raw chain metadata.

    The enriched columns are:

    - is_op_chain (bool)
    - alignment (string)

    See constants for possible alignment values.
    """

    def clean_column(col: str, datatype) -> pl.Expr:
        result: pl.Expr

        # Strip whitespace for all string columns.
        if datatype == datatypes.String:
            result = pl.col(col).str.strip_chars().alias(col)
            result = pl.when(result.str.len_chars() == 0).then(None).otherwise(result).alias(col)
        else:
            result = pl.col(col)

        # Transform dates.
        if col in {"public_mainnet_launch_date", "op_chain_start"}:
            result = (
                result.str.to_date("%m/%d/%y", strict=False).dt.to_string("%Y-%m-%d").alias(col)
            )

        # Cast the block time.
        elif col == "block_time_sec":
            result = result.cast(pl.Float64, strict=False).alias("block_time_sec")

        # Cast the mainnet chain id
        elif col == "mainnet_chain_id":
            result = result.cast(pl.Int64, strict=False).alias("mainnet_chain_id")

        return result

    transformed_cols = [
        clean_column(col, datatype) for col, datatype in raw_df.collect_schema().items()
    ]

    is_op_chain = clean_column("chain_type", pl.String).is_not_null().alias("is_op_chain")

    alignment_col = (
        pl.when(is_op_chain).then(pl.lit(OP_CHAIN)).otherwise(pl.lit(OP_FORK)).alias("alignment")
    )

    return raw_df.select(transformed_cols + [is_op_chain, alignment_col])
