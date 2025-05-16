from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary, last_n_days
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.postgres.client import PostgresClient
from op_analytics.coreutils.env.vault import env_get

from .dataaccess import PlatformMetrics

log = structlog.get_logger()


@dataclass
class PostgresDailyPull:
    """Daily pull of platform metrics."""

    jobs_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch Platform metrics daily datasets."""

        client = PostgresClient(
            instance_connection_name=env_get("POSTGRES_INSTANCE"),
            db_name=env_get("POSTGRES_DB"),
            db_user=env_get("POSTGRES_USER"),
        )

        current_dt: str = now_dt()
        query = "SELECT * FROM public.jobs"

        jobs_raw_data = client.run_query(query)
        jobs_df = pl.DataFrame(
            jobs_raw_data,
            schema={
                "id": pl.String,
                "pipeline_id": pl.String,
                "workflow_id": pl.String,
                "workflow_name": pl.String,
                "number": pl.Int64,
                "name": pl.String,
                "status": pl.String,
                "started_at": pl.Date(),
                "stopped_at": pl.Date(),
            },
        ).rename({"started_at": "dt"})
        jobs_df_truncated = last_n_days(
            jobs_df,
            n_dates=7,
            reference_dt=current_dt,
            date_column_type_is_str=True,
        )

        return PostgresDailyPull(
            # Use the full dataframe when backfilling:
            jobs_df=jobs_df,
            # Use truncated dataframe when running daily:
            # jobs_df=jobs_df_truncated,
        )
