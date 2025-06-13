from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.time import now_date

from ..dataaccess import ChainsMeta
from .chains import ManyChainsSystemConfig
from .chainslist import ChainsList
from .transform import transform_system_config

log = structlog.get_logger()


def execute_pull(process_dt: date | None = None):
    """Pull and store system config data for all chains for the given date (or today)."""

    # Get the list of chains we will fetch from RPC
    chains = ChainsList.fetch()

    # Run the RPC calls in parallel
    chains_system_config = ManyChainsSystemConfig.fetch(chains)

    # Transform the data into the final format
    output_df = transform_system_config(
        system_config_fetched_df=chains_system_config.data,
        chains_df=chains.data,
        process_dt=process_dt or now_date(),
    )

    # Check schema
    raise_for_schema_mismatch(
        actual_schema=output_df.schema,
        expected_schema=SYSTEM_CONFIG_SCHEMA,
    )

    # Load to GCS.
    ChainsMeta.SYSTEM_CONFIG_LIST.write(
        dataframe=output_df,
        sort_by=["chain_id"],
    )
    log.info(f"Stored {len(output_df)} system config records to GCS")

    return output_df


# Schema for system config data
SYSTEM_CONFIG_SCHEMA = pl.Schema(
    {
        "name": pl.String,
        "identifier": pl.String,
        "chain_id": pl.Int64,
        "rpc_url": pl.String,
        "system_config_proxy": pl.String,
        "block_number": pl.Int64,
        "block_timestamp": pl.Int64,
        "batch_inbox_slot": pl.String,
        "dispute_game_factory_slot": pl.String,
        "l1_cross_domain_messenger_slot": pl.String,
        "l1_erc721_bridge_slot": pl.String,
        "l1_standard_bridge_slot": pl.String,
        "optimism_mintable_erc20_factory_slot": pl.String,
        "optimism_portal_slot": pl.String,
        "start_block_slot": pl.String,
        "unsafe_block_signer_slot": pl.String,
        "version": pl.Utf8,  # Changed to String to handle large 256-bit integers, nullable if not supported
        "basefee_scalar": pl.Int64,
        "batch_inbox": pl.Utf8,
        "batcher_hash": pl.Utf8,
        "blob_basefee_scalar": pl.Int64,
        "dispute_game_factory": pl.Utf8,
        "eip1559_denominator": pl.Int64,
        "eip1559_elasticity": pl.Int64,
        "gas_limit": pl.Int64,
        "l1_cross_domain_messenger": pl.Utf8,
        "l1_erc721_bridge": pl.Utf8,
        "l1_standard_bridge": pl.Utf8,
        "maximum_gas_limit": pl.Int64,
        "minimum_gas_limit": pl.Int64,
        "operator_fee_constant": pl.Int64,  # Null for chains that don't support this method
        "operator_fee_scalar": pl.Int64,  # Null for chains that don't support this method
        "optimism_mintable_erc20_factory": pl.Utf8,
        "optimism_portal": pl.Utf8,
        "overhead": pl.Utf8,  # Changed to String to handle large 256-bit integers, nullable if not supported
        "owner": pl.Utf8,
        "scalar": pl.Utf8,  # Changed to String to handle large 256-bit integers, nullable if not supported
        "start_block": pl.Utf8,
        "unsafe_block_signer": pl.Utf8,
        "version_hex": pl.Utf8,
        "dt": pl.String,
    }
)
