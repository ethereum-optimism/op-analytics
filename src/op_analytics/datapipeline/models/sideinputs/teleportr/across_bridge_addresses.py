from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext

from op_analytics.datapipeline.chains.goldsky_chains import goldsky_mainnet_chains_df
from op_analytics.datapipeline.chains.across_bridge import load_across_bridge_addresses


def load(ctx: DuckDBContext):
    goldsky_df = goldsky_mainnet_chains_df()
    across_bridge_addresses_df = load_across_bridge_addresses(chains_df=goldsky_df)

    return across_bridge_addresses_df
