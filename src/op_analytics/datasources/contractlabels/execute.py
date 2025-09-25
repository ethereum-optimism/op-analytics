from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT
from op_analytics.coreutils.misc import raise_for_schema_mismatch

import polars as pl
from .dataaccess import ContractLabels
from .growthepie import GrowThePieLabels
from .oli import OLILabels
from .manual.fetch import ManualLabels
from .opatlas import OPAtlasLabels


def execute_pull():
    """Pull the latest contract labels from the various source."""

    df = ManualLabels.fetch()
    ContractLabels.MANUAL.write(validate(df), sort_by=["address", "chain_id"])

    df = GrowThePieLabels.fetch()
    ContractLabels.GROWTHEPIE.write(validate(df), sort_by=["address", "chain_id"])

    df = OLILabels.fetch()
    ContractLabels.OLI.write(validate(df), sort_by=["address", "chain_id"])

    df = OPAtlasLabels.fetch()
    ContractLabels.OPATLAS.write(validate(df), sort_by=["address", "chain_id"])


CONTRACT_LABELS_DF_SCHEMA = {
    "address": pl.String,
    "chain_id": pl.Int32,
    "label_source": pl.String,
    "project_name": pl.String,
    "contract_name": pl.String,
    "metadata": pl.String,
}


def validate(df: pl.DataFrame) -> pl.DataFrame:
    """Validate the contract labels dataframe."""
    # Make sure the schem is as expected.
    raise_for_schema_mismatch(
        actual_schema=df.schema,
        expected_schema=pl.Schema(CONTRACT_LABELS_DF_SCHEMA),
    )

    # Ensure that addresses are lower case and have the proper length.
    # TODO: Add more checks.
    errors_df = pl.concat(
        [
            df.select(
                pl.lit("wrong_address").alias("error_msg"),
                (pl.col("address").str.len() != 42).alias("is_error"),
            ),
            df.select(
                pl.lit("not_lowercase").alias("error_msg"),
                (pl.col("address").str.to_lowercase() != pl.col("address")).alias("is_error"),
            ),
            # TODO: Check that there are no duplicate contract address.
        ]
    )

    errors = errors_df.filter(pl.col("is_error")).to_dicts()
    if errors:
        raise ValueError(f"Invalid contract labels: {errors}")

    return df.with_columns(dt=pl.lit(DEFAULT_DT))
