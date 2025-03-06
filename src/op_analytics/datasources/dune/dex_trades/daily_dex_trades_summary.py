from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.time import now_dt

from ..dataaccess import Dune

log = structlog.get_logger()

DEX_TRADES_QUERY_ID = 4724100
N_DAYS = 7


@dataclass
class DuneDexTradesSummary:
    """Summary of daily dex trades from Dune."""

    summary_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch Dune dex trades summary."""
        # perform new query execution and get results
        current_dt: str = now_dt()

        df = spice.query(DEX_TRADES_QUERY_ID, refresh=True, parameters={"trailing_days": N_DAYS})
        summary_df = pl.DataFrame(df).rename({"block_date": "dt"})

        summary_df_truncated = last_n_days(
            summary_df,
            n_dates=N_DAYS,
            reference_dt=current_dt,
            date_column_type_is_str=True,
        )
        return DuneDexTradesSummary(summary_df=summary_df_truncated)


def execute_pull():
    """Fetch and write to GCS."""
    data = DuneDexTradesSummary.fetch()

    Dune.DEX_TRADES.write(dataframe=data.summary_df, sort_by=["dt"])

    return {
        "summary_df": dt_summary(data.summary_df),
    }
