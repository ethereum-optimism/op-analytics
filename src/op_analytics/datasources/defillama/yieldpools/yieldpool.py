from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.time import dt_fromisostr


from .metadata import YieldPoolsMetadata

YIELD_POOL_CHART_ENDPOINT = "https://pro-api.llama.fi/{api_key}/yields/chart/{pool}"

YIELD_POOL_SCHEMA = pl.Schema(
    [
        ("dt", pl.String()),
        ("pool", pl.String()),
        ("protocol_slug", pl.String()),
        ("chain", pl.String()),
        ("symbol", pl.String()),
        ("underlying_tokens", pl.List(pl.String())),
        ("reward_tokens", pl.List(pl.String())),
        ("il_risk", pl.String()),
        ("is_stablecoin", pl.Boolean()),
        ("exposure", pl.String()),
        ("pool_meta", pl.String()),
        ("tvl_usd", pl.Float64()),
        ("apy", pl.Float64()),
        ("apy_base", pl.Float64()),
        ("apy_reward", pl.Float64()),
    ]
)


@dataclass
class YieldPool:
    """Historical data for a single Yield Pool."""

    df: pl.DataFrame

    @classmethod
    def fetch(cls, session, pool_id: str, pools_metadata: YieldPoolsMetadata) -> "YieldPool":
        # Fetch data

        api_key = env_get("DEFILLAMA_API_KEY")
        url = YIELD_POOL_CHART_ENDPOINT.format(api_key=api_key, pool=pool_id)
        response = get_data(
            session,
            url,
            retry_attempts=3,
            emit_log=False,  # dont emit logs because the key is in the URL.
        )

        return cls.of(
            data=response["data"],
            pool_id=pool_id,
            pools_metadata=pools_metadata,
        )

    @classmethod
    def of(cls, data, pool_id: str, pools_metadata: YieldPoolsMetadata) -> "YieldPool":
        records = []
        metadata = pools_metadata.get_single_pool_metadata(pool_id)
        for entry in data:
            key = {
                "dt": dt_fromisostr(entry["timestamp"]),
                "pool": pool_id,
            }
            values = {
                "tvl_usd": float(entry.get("tvlUsd", 0.0)),
                "apy": float(entry.get("apy", 0.0)),
                "apy_base": float(entry.get("apyBase") or 0.0),
                "apy_reward": float(entry.get("apyReward") or 0.0),
            }

            records.append(key | metadata | values)

        return cls(df=pl.DataFrame(records, schema=YIELD_POOL_SCHEMA, strict=True))
