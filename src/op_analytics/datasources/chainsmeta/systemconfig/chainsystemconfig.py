from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.clickhouse.client import init_client
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session

from .systemconfig import SystemConfig

log = structlog.get_logger()

# ClickHouse columns for insert
COLUMNS = [
    "process_dt",
    "name",
    "identifier",
    "chain_id",
    "rpc_url",
    "system_config_proxy",
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
]


COLUMN_TYPE_NAMES = [
    "Date",  # process_dt
    "String",  # name
    "String",  # identifier
    "Int32",  # chain_id
    "String",  # rpc_url (chain's RPC)
    "FixedString(66)",  # system_config_proxy
    "FixedString(66)",  # batch_inbox_slot
    "FixedString(66)",  # dispute_game_factory_slot
    "FixedString(66)",  # l1_cross_domain_messenger_slot
    "FixedString(66)",  # l1_erc721_bridge_slot
    "FixedString(66)",  # l1_standard_bridge_slot
    "FixedString(66)",  # optimism_mintable_erc20_factory_slot
    "FixedString(66)",  # optimism_portal_slot
    "FixedString(66)",  # start_block_slot
    "FixedString(66)",  # unsafe_block_signer_slot
    "UInt256",  # version
    "UInt32",  # basefee_scalar
    "FixedString(42)",  # batch_inbox
    "FixedString(66)",  # batcher_hash
    "UInt64",  # blob_basefee_scalar
    "FixedString(42)",  # dispute_game_factory
    "UInt32",  # eip1559_denominator
    "UInt32",  # eip1559_elasticity
    "UInt64",  # gas_limit
    "FixedString(42)",  # l1_cross_domain_messenger
    "FixedString(42)",  # l1_erc721_bridge
    "FixedString(42)",  # l1_standard_bridge
    "UInt64",  # maximum_gas_limit
    "UInt64",  # minimum_gas_limit
    "UInt64",  # operator_fee_constant
    "UInt32",  # operator_fee_scalar
    "FixedString(42)",  # optimism_mintable_erc20_factory
    "FixedString(42)",  # optimism_portal
    "UInt256",  # overhead
    "FixedString(42)",  # owner
    "UInt256",  # scalar
    "FixedString(42)",  # start_block
    "FixedString(42)",  # unsafe_block_signer
    "String",  # version_hex
]

# Chain-dependent delay between RPC requests
SPEED_BUMP = {
    "mainnet/op": 1.0,
    "mainnet/worldchain": 1.0,
}

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
        """Call RPC and insert.

        Call a chain's system config contract and store the results.

        The system config contract is a single contract that contains all the
        system config data for a chain.
        """
        session = new_session()
        client = init_client("OPLABS")

        # Create SystemConfig instance from the proxy address
        system_config = SystemConfig(system_config_proxy=self.system_config_proxy)

        data = []
        config_metadata = system_config.call_rpc(
            rpc_endpoint=self.rpc_url,
            session=session,
            speed_bump=SPEED_BUMP.get(
                self.identifier, DEFAULT_SPEED_BUMP
            ),  # avoid hitting the RPC rate limit
        )

        if config_metadata is None:
            # an error was encountered
            log.warning(f"error encountered for chain {self.identifier}")
            return 0

        row = config_metadata.to_dict()
        row["process_dt"] = process_dt
        row["name"] = self.name
        row["identifier"] = self.identifier
        row["chain_id"] = self.chain_id
        row["rpc_url"] = self.rpc_url
        row["system_config_proxy"] = self.system_config_proxy

        data.append([row[_] for _ in COLUMNS])

        log.info(f"fetched system config metadata from rpc for chain {self.identifier}")

        result = client.insert(
            table="chainsmeta.fact_chain_system_config_v1",
            data=data,
            column_names=COLUMNS,
            column_type_names=COLUMN_TYPE_NAMES,
        )
        log.info(
            f"inserted system config metadata for chain {self.identifier}, {result.written_rows} written rows"
        )

        return result.written_rows
