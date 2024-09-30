import polars as pl
import typer
from op_coreutils.logger import LOGGER
from op_coreutils.path import repo_path
from op_coreutils.storage.gcs import gcs_upload_csv
from polars import datatypes
from polars.functions.col import Col
from typing_extensions import Annotated

log = LOGGER.get_logger()


OP_CHAIN = "OP Chain"
OP_FORK = "OP Stack fork"


DEFAULT_CHAIN_METADATA_LOCATION = repo_path("op_chains_tracking/inputs/chain_metadata_raw.csv")


def load_chain_metadata(path: str | None = None) -> pl.DataFrame:
    path = path or DEFAULT_CHAIN_METADATA_LOCATION

    with open(path, "r") as fcsv:
        raw_df = pl.scan_csv(fcsv)
        log.info(f"Loaded OP chains metadata from {path}")
        return _clean(raw_df).collect()


def goldsky_chains(path: str | None = None):
    df = load_chain_metadata(path)
    return sorted(df.filter(pl.col("oplabs_db_schema").is_not_null())["oplabs_db_schema"].to_list())


def upload_metadata(
    path: Annotated[
        str, typer.Argument(help="Path to local file that has the raw chain metadata.")
    ],
):
    """Upload raw chain metadata csv file.


    The chain_metadata_raw.csv file is maintained manually by the OP Labs team. This function
    accepts a local CSV file with raw chain metadata. It loads the data, cleans it up and uploads
    it to GCS, Dune, Clickhouse and BigQuery.
    """
    clean_df = load_chain_metadata(path)
    gcs_upload_csv("op_chains_tracking/chain_metadata.csv", clean_df)


def _clean(raw_df: pl.DataFrame):
    """Clean and enrich the raw chain metadata.

    The enriched columns are:

    - is_op_chain (bool)
    - alignment (string)

    See constants for possible alignment values.
    """

    def clean_column(col, datatype):
        result: Col

        # Strip whitespace for all string columns.
        if datatype == datatypes.String:
            result = pl.col(col).str.strip_chars().alias(col)
        else:
            result = pl.col(col)

        # Transform dates.
        if col in {"public_mainnet_launch_date", "op_chain_start"}:
            result = (
                result.str.to_date("%m/%d/%y", strict=False).dt.to_string("%Y-%m-%d").alias(col)
            )

        # Cast the block time.
        if col == "block_time_sec":
            result = result.cast(pl.Float64)

        return result

    transformed_cols = [
        clean_column(col, datatype) for col, datatype in raw_df.collect_schema().items()
    ]

    is_op_chain = pl.col("chain_type").is_not_null().alias("is_op_chain")

    alignment_col = (
        pl.when(is_op_chain).then(pl.lit(OP_CHAIN)).otherwise(pl.lit(OP_FORK)).alias("alignment")
    )

    return raw_df.select(transformed_cols + [is_op_chain, alignment_col])
