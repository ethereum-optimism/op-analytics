from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.request import new_session

from ..dataaccess import ChainsMeta
from .systemconfig import SystemConfig

log = structlog.get_logger()

ETHEREUM_RPC_URL = "https://ethereum-rpc.publicnode.com"

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
        "dt": pl.Date,
    }
)

DEFAULT_SPEED_BUMP = 0.4


@dataclass
class ChainSystemConfig:
    """A single system config for a blockchain."""

    name: str
    identifier: str
    chain_id: int
    rpc_url: str
    system_config_proxy: str

    def fetch(self, process_dt: date):
        """Call RPC and return system config data.

        Call a chain's system config contract and return the results as a dictionary.

        The system config contract is a single contract that contains all the
        system config data for a chain.
        """
        session = new_session()

        # Create SystemConfig instance from the proxy address
        system_config = SystemConfig(system_config_proxy=self.system_config_proxy)

        config_metadata = system_config.call_rpc(
            rpc_endpoint=ETHEREUM_RPC_URL,
            session=session,
            speed_bump=DEFAULT_SPEED_BUMP,
        )

        if config_metadata is None:
            # an error was encountered
            log.warning(f"error encountered for chain {self.identifier}")
            return None

        row = config_metadata.to_dict()
        row["name"] = self.name
        row["identifier"] = self.identifier
        row["chain_id"] = self.chain_id
        row["rpc_url"] = self.rpc_url
        row["system_config_proxy"] = self.system_config_proxy
        row["dt"] = process_dt

        # Convert large integers to strings to avoid Polars overflow, handle None values
        row["scalar"] = str(row["scalar"]) if row["scalar"] is not None else None
        row["overhead"] = str(row["overhead"]) if row["overhead"] is not None else None
        row["version"] = str(row["version"]) if row["version"] is not None else None

        log.info(f"fetched system config metadata from rpc for chain {self.identifier}")

        return row


@dataclass
class SystemConfigList:
    """System config data."""

    system_config_df: pl.DataFrame

    def store_system_config_data(system_config_data: list):
        """Store system config data to GCS."""
        if not system_config_data:
            log.warning("No system config data to store")
            return

        # Create DataFrame from the collected data
        system_config_df = pl.DataFrame(system_config_data)

        # reorder columns
        system_config_df = system_config_df.select(
            "name",
            "identifier",
            "chain_id",
            "rpc_url",
            "system_config_proxy",
            "block_number",
            "block_timestamp",
            "batch_inbox_slot",
            "dispute_game_factory_slot",
            "l1_cross_domain_messenger_slot",
            "l1_erc721_bridge_slot",
            "l1_standard_bridge_slot",
            "optimism_mintable_erc20_factory_slot",
            "optimism_portal_slot",
            "start_block_slot",
            "unsafe_block_signer_slot",
            "version",
            "basefee_scalar",
            "batch_inbox",
            "batcher_hash",
            "blob_basefee_scalar",
            "dispute_game_factory",
            "eip1559_denominator",
            "eip1559_elasticity",
            "gas_limit",
            "l1_cross_domain_messenger",
            "l1_erc721_bridge",
            "l1_standard_bridge",
            "maximum_gas_limit",
            "minimum_gas_limit",
            "operator_fee_constant",
            "operator_fee_scalar",
            "optimism_mintable_erc20_factory",
            "optimism_portal",
            "overhead",
            "owner",
            "scalar",
            "start_block",
            "unsafe_block_signer",
            "version_hex",
            "dt",
        )

        # Convert address columns to lowercase
        address_columns = [
            col
            for col in system_config_df.columns
            if "address" in col.lower()
            or col.endswith("_proxy")
            or col
            in [
                "batch_inbox",
                "dispute_game_factory",
                "l1_cross_domain_messenger",
                "l1_erc721_bridge",
                "l1_standard_bridge",
                "optimism_mintable_erc20_factory",
                "optimism_portal",
                "owner",
                "start_block",
                "unsafe_block_signer",
            ]
        ]

        for col in address_columns:
            if col in system_config_df.columns:
                system_config_df = system_config_df.with_columns(pl.col(col).str.to_lowercase())

        # Check the final schema is as expected
        raise_for_schema_mismatch(
            actual_schema=system_config_df.schema,
            expected_schema=SYSTEM_CONFIG_SCHEMA,
        )
        ChainsMeta.SYSTEM_CONFIG_LIST.write(
            dataframe=system_config_df,
            sort_by=["chain_id"],
        )

        log.info(f"Stored {len(system_config_data)} system config records to GCS")

        return SystemConfigList(system_config_df=system_config_df)
