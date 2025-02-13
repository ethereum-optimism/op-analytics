from dataclasses import dataclass
from datetime import date

import polars as pl

from op_analytics.coreutils.request import get_data
from op_analytics.coreutils.time import dt_fromepoch, epoch_is_date, now_date
from op_analytics.coreutils.misc import raise_for_schema_mismatch

from ..dataaccess import DefiLlama


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

    tvl_df: pl.DataFrame | None
    token_tvl_df: pl.DataFrame | None

    def write(self, process_dt: date | None):
        process_dt = process_dt or now_date()

        DefiLlama.PROTOCOLS_TVL.write_to_clickhouse_buffer(
            self.tvl_df.with_columns(process_dt=pl.lit(process_dt))
        )
        DefiLlama.PROTOCOLS_TOKEN_TVL.write_to_clickhouse_buffer(
            self.token_tvl_df.with_columns(process_dt=pl.lit(process_dt))
        )

    @classmethod
    def fetch(cls, session, slug: str):
        # Fetch data
        url = PROTOCOL_DETAILS_ENDPOINT.format(slug=slug)
        data = get_data(session, url, retry_attempts=5)

        # Each app entry can have tvl data in multiple chains. Loop through each chain
        chain_tvls = data.get("chainTvls", {})

        # TVL dataframe
        tvl_df = cls.to_tvl_df(slug, chain_tvls)

        # Token TVL dataframe. Join the token and USD values.
        tokens = cls.to_token_tvl_df(slug, chain_tvls)
        tokens_usd = cls.to_token_tvl_usd_df(slug, chain_tvls)
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

    @staticmethod
    def to_tvl_df(slug: str, chain_tvls: dict):
        tvl_records = []

        for chain, chain_data in chain_tvls.items():
            for tvl_entry in chain_data.get("tvl", []):
                dateval = dt_fromepoch(tvl_entry["date"])

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

    @staticmethod
    def to_token_tvl_df(slug: str, chain_tvls: dict):
        tokens = []

        for chain, chain_data in chain_tvls.items():
            for tokens_entry in chain_data.get("tokens", []):
                dateval = dt_fromepoch(tokens_entry["date"])

                if not epoch_is_date(tokens_entry["date"]):
                    continue

                token_tvls = tokens_entry.get("tokens", [])
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

    @staticmethod
    def to_token_tvl_usd_df(slug: str, chain_tvls: dict):
        tokens_usd = []

        for chain, chain_data in chain_tvls.items():
            for tokens_usd_entry in chain_data.get("tokensInUsd", []):
                dateval = dt_fromepoch(tokens_usd_entry["date"])

                if not epoch_is_date(tokens_usd_entry["date"]):
                    continue

                token_usd_tvls = tokens_usd_entry.get("tokens", [])
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
