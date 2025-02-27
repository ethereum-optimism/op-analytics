from dataclasses import dataclass

import polars as pl
import spice

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary, last_n_days
from op_analytics.coreutils.time import now_dt

from .dataaccess import Dune

log = structlog.get_logger()

DEX_TRADES_QUERY_ID = 4724100


@dataclass
class DuneDexTradesSummary:
    """Summary of daily dex trades from Dune."""

    summary_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch Dune dex trades summary."""
        # perform new query execution and get results
        df = spice.query(DEX_TRADES_QUERY_ID, refresh=True)
        return DuneDexTradesSummary(summary_df=df)


def execute_pull():
    """Fetch and write to GCS."""
    data = DuneDexTradesSummary.fetch()

    Dune.DEX_TRADES.write(dataframe=data.summary_df, sort_by=["dt"])

    return {
        "summary_df": dt_summary(data.summary_df),
    }
