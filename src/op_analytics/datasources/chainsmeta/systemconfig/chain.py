from dataclasses import dataclass

import requests

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session

from .rpc import RPCManager

log = structlog.get_logger()


ETHEREUM_RPC_URL = "https://ethereum-rpc.publicnode.com"
DEFAULT_SPEED_BUMP = 0.4


def _int_to_str(val):
    return str(val) if val is not None else None


@dataclass
class ChainSystemConfig:
    """A single system config for a blockchain."""

    data: dict

    @classmethod
    def fetch(
        cls,
        chain_id: int,
        system_config_proxy: str,
        session: requests.Session | None = None,
    ) -> "ChainSystemConfig":
        """Call RPC and return system config data as a dict, or None on error."""
        session = session or new_session()

        system_config = RPCManager(system_config_proxy=system_config_proxy)
        config_metadata = system_config.call_rpc(
            rpc_endpoint=ETHEREUM_RPC_URL,
            session=session,
            speed_bump=DEFAULT_SPEED_BUMP,
        )

        if config_metadata is None:
            raise Exception(f"error encountered for chain {chain_id}")

        row = config_metadata.to_dict()
        row.update(
            {
                "chain_id": chain_id,
                "system_config_proxy": system_config_proxy,
                # Convert large ints to str for polars
                "scalar": _int_to_str(row.get("scalar")),
                "overhead": _int_to_str(row.get("overhead")),
                "version": _int_to_str(row.get("version")),
            }
        )
        log.info(f"fetched system config metadata from rpc for chain {chain_id}")
        return cls(data=row)
