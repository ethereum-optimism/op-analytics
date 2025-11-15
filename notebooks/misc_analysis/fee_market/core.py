import polars as pl
from typing import Tuple, Dict, Any

def compute_next_base_fee_column(
    blocks_df: pl.DataFrame,
    eip1559_elasticity: int,
    eip1559_denominator: int,
) -> pl.DataFrame:
    """
    Adds 'predicted_next_base_fee_per_gas' to a Polars DataFrame using EIP-1559.
    Expects columns: 'base_fee_per_gas', 'gas_used', 'gas_limit'.
    """
    df = blocks_df

    # compute target_gas, delta, and predicted_next_base_fee_per_gas
    df = df.with_columns([
        (pl.col("gas_limit") // eip1559_elasticity).alias("target_gas"),
        (
            (pl.col("base_fee_per_gas") * (pl.col("gas_used") - (pl.col("gas_limit") // eip1559_elasticity)))
            // ((pl.col("gas_limit") // eip1559_elasticity) * eip1559_denominator)
        ).alias("delta"),
    ])

    df = df.with_columns(
        (pl.col("base_fee_per_gas") + pl.col("delta")).clip(0).alias("predicted_next_base_fee_per_gas")
    )

    return df



def validate_next_base_fee(blocks_df: pl.DataFrame) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    Compare each block's predicted_next_base_fee_per_gas to the next block's actual base_fee_per_gas.
    Expects columns: number, base_fee_per_gas, predicted_next_base_fee_per_gas.
    Returns: (updated_df_with_validation_cols, summary_dict)
    """
    df = (
        blocks_df
        .with_columns([
            pl.col("base_fee_per_gas").shift(-1).alias("actual_next_base_fee_per_gas"),
            pl.col("number").shift(-1).alias("next_block_number"),
        ])
        .with_columns(
            (pl.col("actual_next_base_fee_per_gas") - pl.col("predicted_next_base_fee_per_gas")).alias("diff")
        )
    )

    mismatches_df = df.filter(pl.col("diff") != 0)

    summary = {
        "rows_compared": max(df.height - 1, 0),  # last row has no next block
        "mismatches": mismatches_df.height,
        "first_mismatches": mismatches_df.select(
            [
                "number",
                "next_block_number",
                "predicted_next_base_fee_per_gas",
                "actual_next_base_fee_per_gas",
                "diff",
            ]
        ).head(10),
    }

    return df, summary
