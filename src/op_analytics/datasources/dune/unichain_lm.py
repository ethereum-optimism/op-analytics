from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .dataaccess import Dune
from .utils import determine_lookback

from datetime import datetime

log = structlog.get_logger()

UNI_LM_QUERY_ID = 5084901


@dataclass
class DuneUniLMSummary:
    """Summary of daily dex trades from Dune."""

    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        min_dt: str | None = None,
        max_dt: str | None = None,
    ) -> "DuneUniLMSummary":
        """Fetch Dune Uniswap LM summary."""

        df = (
            spice.query(
                UNI_LM_QUERY_ID,
                # Runs every time so we are guaranteed to get fresh results.
                refresh=True,
                parameters={
                    "start_date": min_dt + ' 00:00:00',
                    "end_date": max_dt + ' 00:00:00',
                },
                api_key=env_get("DUNE_API_KEY"),
                cache=False,
                performance="large",
            )
            .with_columns(
                pl.col("period")
                .str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False)
                .cast(pl.Utf8)
                .alias("dt")
            )
            .drop("period")
        )

        return DuneUniLMSummary(df=df)


def execute_pull(
    min_dt: str | None = '2025-01-01', # Run All-Time as default
    max_dt: str | None = '2035-01-01',
):

    """Fetch and write to GCS."""
    result = DuneUniLMSummary.fetch(
        min_dt=min_dt,
        max_dt=max_dt,
    )

    Dune.UNI_LM_2025.write(
        dataframe=result.df,
        sort_by=["dt"],
    )

    return {
        "df": dt_summary(result.df),
    }
