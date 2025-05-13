from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary

from .dataaccess import Dune


log = structlog.get_logger()

UNI_LM_QUERY_ID = 5084901


@dataclass
class DuneUniLMSummary:
    """Summary of daily uniswap LM program data (2025) from Dune."""

    df: pl.DataFrame

    @classmethod
    def fetch(
        cls,
        min_dt: str | None = "2025-01-01",  # Run All-Time as default
        max_dt: str | None = "2035-01-01",
    ) -> "DuneUniLMSummary":
        """Fetch Dune Uniswap LM summary."""

        # Ensure we have valid datetime strings
        start_date = f"{min_dt} 00:00:00" if min_dt is not None else "2025-01-01 00:00:00"
        end_date = f"{max_dt} 00:00:00" if max_dt is not None else "2035-01-01 00:00:00"

        df = (
            spice.query(
                query_or_execution=UNI_LM_QUERY_ID,
                refresh=True,
                parameters={
                    "start_date": start_date,
                    "end_date": end_date,
                },
                api_key=env_get("DUNE_API_KEY"),
                cache=False,
                performance="large",
                poll=True,  # Required to get DataFrame result
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
    min_dt: str | None = "2025-01-01",  # Run All-Time as default
    max_dt: str | None = "2035-01-01",
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
