from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .dataaccess import Dune
from .utils import determine_lookback

log = structlog.get_logger()

DEX_TRADES_QUERY_ID = 4724100
N_DAYS = 7


@dataclass
class DuneDexTradesSummary:
    """Summary of daily dex trades from Dune."""

    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        min_dt: str | None = None,
        max_dt: str | None = None,
    ) -> "DuneDexTradesSummary":
        """Fetch Dune dex trades summary."""

        lookback_start, lookback_end = determine_lookback(
            min_dt=min_dt,
            max_dt=max_dt,
            default_n_days=N_DAYS,
        )

        df = (
            spice.query(
                DEX_TRADES_QUERY_ID,
                # Runs every time so we are guaranteed to get fresh results.
                refresh=True,
                parameters={
                    "lookback_start_days": lookback_start,
                    "lookback_end_days": lookback_end,
                },
                api_key=env_get("DUNE_API_KEY"),
                cache=False,
                performance="large",
            )
            .with_columns(
                pl.col("block_date")
                .str.to_date(format="%Y-%m-%d %H:%M:%S.000 UTC")
                .cast(pl.Utf8)
                .alias("dt")
            )
            .drop("block_date")
        )

        return DuneDexTradesSummary(df=df)


def execute_pull(
    min_dt: str | None = None,
    max_dt: str | None = None,
):
    """Fetch and write to GCS."""
    result = DuneDexTradesSummary.fetch(
        min_dt=min_dt,
        max_dt=max_dt,
    )

    Dune.DEX_TRADES.write(
        dataframe=result.df,
        sort_by=["dt"],
    )

    return {
        "df": dt_summary(result.df),
    }
