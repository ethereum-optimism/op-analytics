def safe_uint256(val):
    """Cap uint256 values to a range that DuckDB can support.

    NOTE: DuckDB does not support converting from python to UHUGEINT. So we have to use UBIGINT.
    https://github.com/duckdb/duckdb/blob/8e68a3e34aa526a342ae91e1b14b764bb3075a12/tools/pythonpkg/src/native/python_conversion.cpp#L325
    """
    if val < 18446744073709551615:
        return val
    return None
