import polars as pl

def get_gas_limit_for_date(date_str: str, gas_limits_df: pl.DataFrame, chain: str = "base") -> int:
    """Get gas limit for date_str; fall back to most recent prior date, else 240M."""
    # exact match
    result = gas_limits_df.filter(pl.col("date_formatted") == date_str)
    if result.height:
        return int(result["gas_limit"][0])

    # previous (<=) by formatted date string (YYYY-MM-DD ordering is lexicographic-safe)
    prev = (
        gas_limits_df
        .filter(pl.col("date_formatted") <= date_str)
        .sort("date_formatted")
        .tail(1)
    )
    if prev.height:
        return int(prev["gas_limit"][0])

    # final fallback if the requested date is before earliest entry
    return None
