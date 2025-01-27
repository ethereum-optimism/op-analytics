from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext

from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains_df


def load(ctx: DuckDBContext):
    return goldsky_mainnet_chains_df()
