from dataclasses import dataclass


import polars as pl

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datasources.chainsmeta.dataaccess import ChainsMeta
from op_analytics.coreutils.logger import structlog


log = structlog.get_logger()


@dataclass
class ChainsList:
    """ "Hold a list of chains for which we will fetch SystemConfig"""

    data: pl.DataFrame

    @classmethod
    def fetch(cls) -> "ChainsList":
        """Find the OP Chains' system configs that need to be updated on a given date.

        Returns a list of ChainSystemConfig objects, one for each chain.
        Uses Ethereum RPC for system config calls since contracts are on L1.
        """
        df_chain_list = ChainsMeta.SUPERCHAIN_CHAIN_LIST.read_polars(
            location=DataLocation.GCS
        ).filter(pl.col("dt") == pl.col("dt").max())

        df_address_list = ChainsMeta.SUPERCHAIN_ADDRESS_LIST.read_polars(
            location=DataLocation.GCS
        ).filter(pl.col("dt") == pl.col("dt").max())

        df_raw = df_chain_list.join(df_address_list, on="chain_id", how="inner").select(
            pl.col("name"),
            pl.col("identifier"),
            pl.col("chain_id"),
            pl.col("rpc").list.first().alias("rpc_url"),
            pl.col("system_config_proxy"),
        )

        df = df_raw.filter(
            (pl.col("rpc_url").is_not_null())
            & (pl.col("rpc_url") != "")
            & (pl.col("system_config_proxy").is_not_null())
            & (pl.col("system_config_proxy") != "")
            & (~pl.col("identifier").str.starts_with("sepolia/"))  # Exclude sepolia testnets
        )

        log.info(f"found {len(df)} chains to update system configs for")
        return cls(data=df)
