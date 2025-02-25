from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import polars as pl
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()

LEND_BORROW_POOLS_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/poolsBorrow"


@dataclass
class LendBorrowPoolsMetadata:
    # Metadata of all pools as a polars dataframe.
    df: pl.DataFrame

    # Metadata indexed by pool ID.
    indexed: dict[str, dict]

    def pool_ids(self) -> list[str]:
        return list(self.indexed.keys())

    def get_single_pool_metadata(self, pool_id: str) -> dict[str, Any]:
        return self.indexed[pool_id]

    @classmethod
    def fetch(
        cls,
        process_dt: date,
        session: requests.Session | None = None,
    ) -> "LendBorrowPoolsMetadata":
        session = session or new_session()
        api_key = env_get("DEFILLAMA_API_KEY")

        response = get_data(
            session,
            LEND_BORROW_POOLS_ENDPOINT.format(api_key=api_key),
            emit_log=False,  # dont emit logs because the key is in the URL.
        )
        log.info("fetched yield pools metadata from defillama")

        breakpoint()

        return cls.of(data=response["data"], process_dt=process_dt)

    @classmethod
    def of(cls, data: list[dict], process_dt: date) -> "LendBorrowPoolsMetadata":
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
                "borrowable": pool["borrowable"],
                "minted_coin": pool["mintedCoin"],
                "borrow_factor": float(pool["borrowFactor"] or np.nan),
                "pool_meta": pool["poolMeta"] or "main_pool",
            }

            records.append(row)
            indexed[pool_id] = row

        return cls(df=pl.DataFrame(records).with_columns(dt=pl.lit(process_dt)), indexed=indexed)
