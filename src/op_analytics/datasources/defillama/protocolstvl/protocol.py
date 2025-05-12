from dataclasses import dataclass
from typing import Any

import polars as pl
import requests

from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.time import dt_fromepoch, epoch_is_date
from op_analytics.coreutils.misc import raise_for_schema_mismatch


PROTOCOL_DETAILS_ENDPOINT = "https://api.llama.fi/protocol/{slug}"


TVL_SCHEMA = {
    "protocol_slug": pl.String(),
    "chain": pl.String(),
    "dt": pl.String(),
    "total_app_tvl": pl.Float64(),
}

TOKEN_TVL_SCHEMA = {
    "protocol_slug": pl.String(),
    "chain": pl.String(),
    "dt": pl.String(),
    "token": pl.String(),
    "app_token_tvl": pl.Float64(),
    "app_token_tvl_usd": pl.Float64(),
}


@dataclass
class ProtocolTVL:
    """Records obtained for a single protocol."""

    tvl_df: pl.DataFrame
    token_tvl_df: pl.DataFrame

    @classmethod
    def fetch(cls, slug: str, session: requests.Session | None = None) -> "ProtocolTVL":
        session = session or new_session()

        url = PROTOCOL_DETAILS_ENDPOINT.format(slug=slug)
        data = get_data(session, url, retry_attempts=2)
        return cls.of(slug=slug, data=data)

    @classmethod
    def of(cls, slug: str, data: dict[str, Any]) -> "ProtocolTVL":
        # Each app entry can have tvl data in multiple chains. Loop through each chain
        chain_tvls = data.get("chainTvls", {})

        try:
            # TVL dataframe
            tvl_df = make_tvl_df(slug, chain_tvls)

            # Token TVL
            tokens = make_token_tvl_df(slug, chain_tvls)

            # Token TVL-USD
            tokens_usd = make_token_tvl_usd_df(slug, chain_tvls)

        except Exception as ex:
            raise Exception(f"Error processing data for slug={slug}") from ex

        # Join TVL and TVL-USD
        token_tvl_df = tokens.join(
            tokens_usd,
            how="full",
            on=["protocol_slug", "chain", "dt", "token"],
            coalesce=True,
        )

        raise_for_schema_mismatch(
            actual_schema=token_tvl_df.schema,
            expected_schema=pl.Schema(TOKEN_TVL_SCHEMA),
        )

        return cls(
            tvl_df=tvl_df,
            token_tvl_df=token_tvl_df,
        )

    def max_dt(self):
        self.tvl_df


def make_tvl_df(slug: str, chain_tvls: dict):
    tvl_records = []

    for chain, chain_data in chain_tvls.items():
        for tvl_entry in chain_data.get("tvl", []):
            epoch = tvl_entry["date"]
            dateval = dt_fromepoch(epoch)

            if not epoch_is_date(tvl_entry["date"]):
                continue

            tvl_records.append(
                {
                    "protocol_slug": slug,
                    "chain": chain,
                    "dt": dateval,
                    "total_app_tvl": float(tvl_entry.get("totalLiquidityUSD")),
                }
            )

    return pl.DataFrame(
        tvl_records,
        schema=TVL_SCHEMA,
    )


def make_token_tvl_df(slug: str, chain_tvls: dict):
    tokens = []

    for chain, chain_data in chain_tvls.items():
        for tokens_entry in chain_data.get("tokens") or []:
            dateval = dt_fromepoch(tokens_entry["date"])

            if not epoch_is_date(tokens_entry["date"]):
                continue

            token_tvls = tokens_entry.get("tokens") or []
            for token in token_tvls:
                tokens.append(
                    {
                        "protocol_slug": slug,
                        "chain": chain,
                        "dt": dateval,
                        "token": token,
                        "app_token_tvl": float(token_tvls[token]),
                    }
                )

    return pl.DataFrame(
        tokens,
        schema={
            "protocol_slug": pl.String(),
            "chain": pl.String(),
            "dt": pl.String(),
            "token": pl.String(),
            "app_token_tvl": pl.Float64(),
        },
    )


def make_token_tvl_usd_df(slug: str, chain_tvls: dict):
    tokens_usd = []

    for chain, chain_data in chain_tvls.items():
        for tokens_usd_entry in chain_data.get("tokensInUsd") or []:
            dateval = dt_fromepoch(tokens_usd_entry["date"])

            if not epoch_is_date(tokens_usd_entry["date"]):
                continue

            token_usd_tvls = tokens_usd_entry.get("tokens") or []
            for token in token_usd_tvls:
                tokens_usd.append(
                    {
                        "protocol_slug": slug,
                        "chain": chain,
                        "dt": dateval,
                        "token": token,
                        "app_token_tvl_usd": float(token_usd_tvls[token]),
                    }
                )

    return pl.DataFrame(
        tokens_usd,
        schema={
            "protocol_slug": pl.String(),
            "chain": pl.String(),
            "dt": pl.String(),
            "token": pl.String(),
            "app_token_tvl_usd": pl.Float64(),
        },
    )
