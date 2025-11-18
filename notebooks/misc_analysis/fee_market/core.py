import polars as pl

def compute_next_base_fee(
    blocks_df: pl.DataFrame,
    eip1559_elasticity: pl.Series | int,
    eip1559_denominator: pl.Series | int,
) -> pl.DataFrame:
    # Normalize params to Python ints
    elasticity_value = int(eip1559_elasticity.item() if isinstance(eip1559_elasticity, pl.Series) else eip1559_elasticity)
    denominator_value = int(eip1559_denominator.item() if isinstance(eip1559_denominator, pl.Series) else eip1559_denominator)

    # Cast to Int64 and compute all columns in one pass
    # EIP-1559 formula: next_base_fee = base_fee + (base_fee * (gas_used - target_gas)) // (target_gas * denominator)
    # where target_gas = gas_limit // elasticity
    elasticity_lit = pl.lit(elasticity_value, dtype=pl.Int64)
    denominator_lit = pl.lit(denominator_value, dtype=pl.Int64)

    base_fee = pl.col("base_fee_per_gas").cast(pl.Int64)
    gas_used = pl.col("gas_used").cast(pl.Int64)
    gas_limit = pl.col("gas_limit").cast(pl.Int64)

    target_gas = gas_limit // elasticity_lit
    # Integer division already truncates toward zero for positive divisors
    base_fee_delta = (base_fee * (gas_used - target_gas)) // target_gas // denominator_lit
    predicted_next = (base_fee + base_fee_delta).clip(0)

    return blocks_df.with_columns([
        target_gas.alias("target_gas"),
        base_fee_delta.alias("base_fee_delta"),
        predicted_next.alias("predicted_next_base_fee_per_gas"),
    ])



def validate_next_base_fee(blocks_df: pl.DataFrame):
    # Sort so shift is well-defined, then compute all derived columns in one pass
    df = (
        blocks_df.sort(["network", "chain_id", "number"])
        .with_columns([
            pl.col("base_fee_per_gas").shift(-1).over(["network", "chain_id"]).alias("actual_next_base_fee_per_gas"),
            pl.col("number").shift(-1).over(["network", "chain_id"]).alias("next_block_number"),
        ])
        .with_columns([
            (pl.col("next_block_number") == (pl.col("number") + 1)).alias("is_consecutive"),
            pl.when(pl.col("next_block_number") == (pl.col("number") + 1))
              .then(pl.col("actual_next_base_fee_per_gas") - pl.col("predicted_next_base_fee_per_gas"))
              .otherwise(None)
              .alias("diff"),
        ])
    )

    # Compute summary efficiently - filter once, then compute both metrics
    consecutive_df = df.filter(pl.col("is_consecutive"))
    summary = {
        "rows_compared": consecutive_df.height,
        "mismatches": consecutive_df.filter(pl.col("diff") > 1).height,
    }
    return df, summary
