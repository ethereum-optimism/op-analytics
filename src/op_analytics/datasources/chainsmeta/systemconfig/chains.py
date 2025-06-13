from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.threads import run_concurrently_store_failures

from .chain import ChainSystemConfig
from .chainslist import ChainsList


log = structlog.get_logger()

ETHEREUM_RPC_URL = "https://ethereum-rpc.publicnode.com"


@dataclass
class ManyChainsSystemConfig:
    """System config data."""

    data: pl.DataFrame

    @classmethod
    def fetch(cls, chains: ChainsList) -> "ManyChainsSystemConfig":
        """Fetch system config data from GCS."""

        # Build the list of parameters for the paralllized RPC calls.
        targets: dict[str, dict] = {}
        for row in chains.data.to_dicts():
            chain_id = row["chain_id"]
            targets[chain_id] = {
                "chain_id": chain_id,
                "system_config_proxy": row["system_config_proxy"],
            }

        # Use lambda for fetch
        summary = run_concurrently_store_failures(
            function=lambda x: ChainSystemConfig.fetch(**x),
            targets=targets,
            max_workers=3,  # Reduced from 8 to 3 to avoid rate limiting
        )

        if summary.failures:
            for chain_id, error_msg in summary.failures.items():
                detail = f"failed chain={chain_id}: error={error_msg}"
                log.error(detail)

            msg = f"{len(summary.failures)} chain,dt tasks failed to execute"
            raise Exception(msg)

        result = pl.DataFrame([result.data for result in summary.results.values()])
        log.info(f"System config pull completed: {len(result)} successful")
        return cls(data=result)
