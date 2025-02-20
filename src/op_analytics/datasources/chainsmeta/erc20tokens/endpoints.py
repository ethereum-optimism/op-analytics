from functools import cache

import polars as pl

from op_analytics.datapipeline.chains.load import load_chain_metadata


@cache
def get_rpc_for_chain(chain_id: int) -> str:
    """Find the RPC endpoint for the chain."""

    df = load_chain_metadata()
    rpcs = (
        df.filter(pl.col("oplabs_db_schema").is_not_null())
        .select(
            pl.col("oplabs_db_schema").alias("chain"),
            pl.col("mainnet_chain_id").alias("chain_id"),
            pl.col("rpc_url"),
        )
        .filter(pl.col("chain_id") == chain_id)
        .to_dicts()
    )
    if len(rpcs) == 0:
        raise Exception(f"could not find rpc for chain_id: {chain_id}")
    return rpcs[0]["rpc_url"]
