from dataclasses import dataclass
from typing import Any

import polars as pl
import requests

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import dt_fromisostr

from .metadata import LendBorrowPoolsMetadata

LEND_BORROW_POOL_CHART_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/chartLendBorrow/{pool}"


LEND_BORROW_POOL_SCHEMA = pl.Schema(
    [
        # Key
        ("dt", pl.String()),
        ("pool", pl.String()),
        # Metadata
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("underlying_tokens", pl.List(pl.String())),
        ("reward_tokens", pl.List(pl.String())),
        ("il_risk", pl.String()),
        ("is_stablecoin", pl.Boolean()),
        ("exposure", pl.String()),
        ("borrowable", pl.Boolean()),
        ("minted_coin", pl.String()),
        ("borrow_factor", pl.Float64()),
        ("pool_meta", pl.String()),
        # Values
        ("total_supply_usd", pl.Int64()),
        ("total_borrow_usd", pl.Int64()),
        ("debt_ceiling_usd", pl.Int64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
        ("apy_base_borrow", pl.Float64()),
        ("apy_reward_borrow", pl.Float64()),
    ]
)


@dataclass
class LendBorrowPool:
    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        pool_id: str,
        metadata: LendBorrowPoolsMetadata,
        session: requests.Session | None = None,
    ) -> "LendBorrowPool":
        session = session or new_session()
        # Fetch data

        api_key = env_get("DEFILLAMA_API_KEY")
        url = LEND_BORROW_POOL_CHART_ENDPOINT.format(api_key=api_key, pool=pool_id)
        response = get_data(
            session,
            url,
            retry_attempts=3,
            emit_log=False,  # dont emit logs because the key is in the URL.
        )

        return cls.of(
            data=response["data"],
            pool_id=pool_id,
            metadata=metadata,
        )

    @classmethod
    def of(
        cls,
        data: list[dict[str, Any]],
        pool_id: str,
        metadata: LendBorrowPoolsMetadata,
    ) -> "LendBorrowPool":
        records = []
        pool_metadata = metadata.get_single_pool_metadata(pool_id)

        for entry in data:
            key = {
                "dt": dt_fromisostr(entry["timestamp"]),
                "pool": pool_id,
            }
            values = {
                "total_supply_usd": int(entry["totalSupplyUsd"] or 0),
                "total_borrow_usd": int(entry["totalBorrowUsd"] or 0),
                "debt_ceiling_usd": int(entry["debtCeilingUsd"] or 0),
                "apy_base": float(entry.get("apyBase") or 0.0),
                "apy_reward": float(entry.get("apyReward") or 0.0),
                "apy_base_borrow": float(entry.get("apyBaseBorrow") or 0.0),
                "apy_reward_borrow": float(entry.get("apyRewardBorrow") or 0.0),
            }

            records.append(key | pool_metadata | values)

        return cls(df=pl.DataFrame(records, schema=LEND_BORROW_POOL_SCHEMA, strict=True))
