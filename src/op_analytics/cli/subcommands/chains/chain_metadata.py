import polars as pl

from polars import datatypes
from polars.functions.col import Col


LEGACY_OP = "Legacy OP Chain"
OP_CHAIN = "OP Chain"
OP_FORK = "OP Stack fork"


def clean(raw_df: pl.DataFrame):
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

    # NOTE (pedro - 2024/09/27) The raw data has the string "legacy" all lower case. So appending
    # legacy to the chain alignment column does not seem to be working as intended at the moment.
    is_legacy = pl.col("op_based_version").str.contains("Legacy")
    is_op_chain = pl.col("chain_type").is_not_null().alias("is_op_chain")

    # NOTE (pedro - 2024/09/27) "Other EVMs" or "OP Chain Legacy" values are never asigned.
    alignment_col = (
        pl.when(is_legacy & is_op_chain)
        .then(pl.lit(LEGACY_OP))
        .when(is_op_chain)
        .then(pl.lit(OP_CHAIN))
        .otherwise(pl.lit(OP_FORK))
        .alias("alignment")
    )

    return raw_df.select(transformed_cols + [is_op_chain, alignment_col])
