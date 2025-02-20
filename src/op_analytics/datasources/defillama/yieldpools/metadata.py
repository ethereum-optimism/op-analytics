from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

YIELD_POOLS_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/pools"


@dataclass
class YieldPoolsMetadata:
    # Metadata of all pools as a polars dataframe.
    df: pl.DataFrame

    # Metadata indexed by pool ID.
    indexed: dict[str, dict]

    def pool_ids(self) -> list[str]:
        return list(self.indexed.keys())

    def get_single_pool_metadata(self, pool_id: str) -> dict[str, Any]:
        return self.indexed[pool_id]

    @classmethod
    def fetch(cls, session, process_dt: date) -> "YieldPoolsMetadata":
        api_key = env_get("DEFILLAMA_API_KEY")

        response = get_data(
            session,
            YIELD_POOLS_ENDPOINT.format(api_key=api_key),
            emit_log=False,  # dont emit logs because the key is in the URL.
        )
        log.info("fetched yield pools metadata from defillama")

        return cls.of(data=response["data"], process_dt=process_dt)

    @classmethod
    def of(cls, data: list[dict], process_dt: date) -> "YieldPoolsMetadata":
        records = []
        indexed = {}

        for pool in data:
            pool_id = pool["pool"]

            row = {
                "pool": pool["pool"],
                "protocol_slug": pool["project"],
                "chain": pool["chain"],
                "symbol": pool["symbol"],
                "underlying_tokens": pool.get("underlyingTokens", []),
                "reward_tokens": pool.get("rewardTokens", []),
                "il_risk": pool["ilRisk"],
                "is_stablecoin": pool["stablecoin"],
                "exposure": pool["exposure"],
                "pool_meta": pool["poolMeta"] or "main_pool",
            }

            records.append(row)
            indexed[pool_id] = row

        return cls(df=pl.DataFrame(records).with_columns(dt=pl.lit(process_dt)), indexed=indexed)
