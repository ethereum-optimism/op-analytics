import polars as pl

from op_analytics.coreutils.bigquery.write import most_recent_dates
from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.misc import raise_for_schema_mismatch
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary
from op_analytics.coreutils.request import new_session
from op_analytics.coreutils.threads import run_concurrently
from op_analytics.coreutils.time import now_dt

from ..dataaccess import DefiLlama
from .chain import Chain
from .metadata import ChainsMetadata

log = structlog.get_logger()


TVL_TABLE_LAST_N_DAYS = 90


CHAIN_DF_SCHEMA = {
    "chain_name": pl.String,
    "dt": pl.String,
    "tvl": pl.Float64,
}


def execute_pull():
    session = new_session()

    metadata = ChainsMetadata.fetch(session)
    DefiLlama.CHAINS_METADATA.write(
        dataframe=metadata.df.with_columns(dt=pl.lit(now_dt())),
        sort_by=["chain_name"],
    )

    # Call the API endpoint for each stablecoin in parallel.
    chains_data: dict[str, Chain] = run_concurrently(
        function=lambda x: Chain.fetch(chain=x, session=session),
        targets=metadata.chains,
        max_workers=4,
    )

    rows = []
    for chain in chains_data.values():
        rows.extend(chain.rows)
    tvl_df = pl.DataFrame(rows, schema=CHAIN_DF_SCHEMA)

    # Schema assertions to help our future selves reading this code.
    raise_for_schema_mismatch(
        actual_schema=tvl_df.schema,
        expected_schema=pl.Schema(CHAIN_DF_SCHEMA),
    )

    # Write balances.
    DefiLlama.HISTORICAL_CHAIN_TVL.write(
        dataframe=most_recent_dates(tvl_df, n_dates=TVL_TABLE_LAST_N_DAYS, date_column="dt"),
        sort_by=["chain_name"],
    )

    return {
        "metadata_df": dt_summary(metadata.df),
        "tvl_df": dt_summary(tvl_df),
    }
