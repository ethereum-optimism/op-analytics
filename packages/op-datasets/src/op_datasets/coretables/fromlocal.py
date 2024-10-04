from op_datasets.blockrange import BlockRange


def read_core_tables(block_range: BlockRange):
    """Get the core dataset tables from a local directory."""

    names = ["blocks", "transactions"]
    queries = [
        get_sql("op", "blocks", filter=block_range.filter()),
        get_sql("op", "transactions", filter=block_range.filter(number_column="block_number")),
    ]
    dataframes = run_queries_concurrently(queries)

    return {names[i]: df for i, df in enumerate(dataframes)}
