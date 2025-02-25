from dataclasses import dataclass
from typing import Iterable

import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import get_data, new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt


from ..dataaccess import DefiLlama
from .stablecoin import StableCoin

log = structlog.get_logger()

SUMMARY_ENDPOINT = "https://stablecoins.llama.fi/stablecoins?includePrices=true"


BALANCES_TABLE_LAST_N_DAYS = 90  # write only recent dates


METADATA_DF_SCHEMA = {
    "id": pl.String,
    "name": pl.String,
    "address": pl.String,
    "symbol": pl.String,
    "url": pl.String,
    "pegType": pl.String,
    "pegMechanism": pl.String,
    "description": pl.String,
    "mintRedeemDescription": pl.String,
    "onCoinGecko": pl.String,
    "gecko_id": pl.String,
    "cmcId": pl.String,
    "priceSource": pl.String,
    "twitter": pl.String,
    "price": pl.Float64,
}

BALANCES_DF_SCHEMA = {
    "id": pl.String(),
    "chain": pl.String(),
    "dt": pl.String(),
    "circulating": pl.Decimal(precision=38, scale=18),
    "bridged_to": pl.Decimal(precision=38, scale=18),
    "minted": pl.Decimal(precision=38, scale=18),
    "unreleased": pl.Decimal(precision=38, scale=18),
    "name": pl.String(),
    "symbol": pl.String(),
}


def execute_pull():
    session = new_session()

    # Call the summary endpoint to find the list of stablecoins tracked by DefiLLama.
    summary = get_data(session, SUMMARY_ENDPOINT)
    stablecoins_summary = summary["peggedAssets"]

    # Call the API endpoint for each stablecoin in parallel.
    stablecoind_ids = [stablecoin["id"] for stablecoin in stablecoins_summary]
    stablecoins_data: dict[str, StableCoin] = run_concurrently(
        function=lambda x: StableCoin.fetch(stablecoin_id=x, session=session),
        targets=stablecoind_ids,
        max_workers=4,
    )

    # Collect dataframes for all stablecoins.
    result = DefillamaStablecoins.of(stablecoins_data.values())

    # Write metadata.
    DefiLlama.STABLECOINS_METADATA.write(
        dataframe=result.metadata_df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["symbol"],
    )

    # Write balances.
    DefiLlama.STABLECOINS_BALANCE.write(
        dataframe=most_recent_dates(result.balances_df, n_dates=BALANCES_TABLE_LAST_N_DAYS),
        sort_by=["symbol", "chain"],
    )

    return {
        "metadata_df": dt_summary(result.metadata_df),
        "balances_df": dt_summary(result.balances_df),
    }


@dataclass
class DefillamaStablecoins:
    """Metadata and balances for all stablecoins.

    This is the result we obtain after fetching from the API and extracting the data
    that we need to ingest.
    """

    metadata_df: pl.DataFrame
    balances_df: pl.DataFrame

    @classmethod
    def of(cls, data: Iterable[StableCoin]) -> "DefillamaStablecoins":
        """Put together metadata and balances dataframes for all stablecoins."""
        metadata: list[dict] = []
        balances: list[dict] = []
        for stablecoin in data:
            metadata.append(stablecoin.metadata)
            balances.extend(stablecoin.balances)

        metadata_df = pl.DataFrame(metadata, schema=METADATA_DF_SCHEMA)
        balances_df = pl.DataFrame(balances, schema=BALANCES_DF_SCHEMA)

        # Schema assertions to help our future selves reading this code.
        raise_for_schema_mismatch(
            actual_schema=metadata_df.schema,
            expected_schema=pl.Schema(METADATA_DF_SCHEMA),
        )

        raise_for_schema_mismatch(
            actual_schema=balances_df.schema,
            expected_schema=pl.Schema(BALANCES_DF_SCHEMA),
        )

        return DefillamaStablecoins(metadata_df=metadata_df, balances_df=balances_df)
