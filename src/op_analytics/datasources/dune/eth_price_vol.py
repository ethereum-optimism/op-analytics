from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .dataaccess import Dune
from .utils import determine_lookback

log = structlog.get_logger()

ETH_PRICE_VOL_QUERY_ID = 5139749
N_DAYS = 7


@dataclass
class DuneETHPriceVOlSummary:
    """Daily ETH Price Information and Volatility Measures"""

    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        min_dt: str | None = None, 
        max_dt: str | None = None,
    ) -> "DuneETHPriceVOlSummary":
        """Fetch ETH Query"""

        lookback_start, lookback_end = determine_lookback(
            min_dt=min_dt,
            max_dt=max_dt,
            default_n_days=N_DAYS,
        )


        df = (
            spice.query(
                query_or_execution=ETH_PRICE_VOL_QUERY_ID,
                refresh=True,
                parameters={
                    "lookback_start_days": lookback_start,
                    "lookback_end_days": lookback_end,
                },
                api_key=env_get("DUNE_API_KEY"),
                cache=False,
                performance="large",
                poll=True,  # Required to get DataFrame result
            )
            .with_columns(
                pl.col("day_dt")
                .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                .cast(pl.Date)
                .cast(pl.Utf8)
                .alias("dt")
            )
            .drop("day_dt")
        )

        return DuneETHPriceVOlSummary(df=df)


def execute_pull(
    min_dt: str | None = None,
    max_dt: str | None = None,
):
    """Fetch and write to GCS."""
    result = DuneETHPriceVOlSummary.fetch(
        min_dt=min_dt,
        max_dt=max_dt,
    )
    print(result)

    Dune.ETH_PRICE_VOL.write(
        dataframe=result.df,
        sort_by=["dt"],
    )

    return {
        "df": dt_summary(result.df),
    }
