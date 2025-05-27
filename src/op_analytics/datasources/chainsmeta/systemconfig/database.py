from datetime import date
import polars as pl

from op_analytics.datapipeline.chains.load import load_chain_metadata
from op_analytics.coreutils.logger import structlog

from .chainsystemconfig import ChainSystemConfig
from .systemconfig import SystemConfig

log = structlog.get_logger()


def find_system_configs(process_dt: date) -> list[ChainSystemConfig]:
    """Find the OP Chains' system configs that need to be updated on a given date.

    Returns a list of ChainSystemConfig objects, one for each chain.
    """
    df_raw = load_chain_metadata()

    df = df_raw.filter(
        pl.col("oplabs_db_schema").is_not_null() & pl.col("system_config_proxy").is_not_null()
    ).select(
        pl.col("oplabs_db_schema").alias("chain"),
        pl.col("mainnet_chain_id").alias("chain_id"),
        pl.col("rpc_url"),
        pl.col("system_config_proxy"),
    )
    log.info(f"found {len(df)} chains to update system configs for")

    result = []
    for row in df.to_dicts():
        result.append(
            ChainSystemConfig(
                chain=row["chain"],
                chain_id=row["chain_id"],
                system_config_proxy=row["system_config_proxy"],
                rpc_endpoint=row["rpc_url"],
            )
        )

    return result
