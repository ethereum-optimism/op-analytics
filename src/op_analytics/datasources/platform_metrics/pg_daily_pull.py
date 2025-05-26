from dataclasses import dataclass

import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import last_n_days
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.postgres.client import PostgresClient
from op_analytics.coreutils.env.vault import env_get

log = structlog.get_logger()


@dataclass
class PostgresDailyPull:
    """Daily pull of platform metrics."""

    jobs_df: pl.DataFrame
    pipelines_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch Platform metrics daily datasets."""

        client = PostgresClient(
            instance_connection_name=env_get("POSTGRES_INSTANCE"),
            db_name=env_get("POSTGRES_DB"),
            db_user=env_get("POSTGRES_USER"),
        )

        current_dt: str = now_dt()
        job_query = "SELECT * FROM public.jobs"
        pipeline_query = "SELECT id, number::VARCHAR, commit, branch, CAST(created_at::VARCHAR AS DATE) as created_at FROM public.pipelines"

        jobs_raw_data = client.run_query(job_query)
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
            date_column_type_is_str=False,
        )

        pipelines_raw_data = client.run_query(pipeline_query)
        pipelines_df = pl.DataFrame(
            pipelines_raw_data,
            schema={
                "id": pl.String,
                "number": pl.String,
                "commit": pl.String,
                "branch": pl.String,
                "created_at": pl.Date(),
            },
        ).rename({"created_at": "dt"})

        pipelines_df_truncated = last_n_days(
            pipelines_df,
            n_dates=7,
            reference_dt=current_dt,
            date_column_type_is_str=False,
        )

        return PostgresDailyPull(
            # Use the full dataframe when backfilling:
            # jobs_df=jobs_df,
            # pipelines_df=pipelines_df,
            # Use truncated dataframe when running daily:
            jobs_df=jobs_df_truncated,
            pipelines_df=pipelines_df_truncated,
        )
