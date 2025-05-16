from dataclasses import dataclass

import polars as pl
import pandas as pd

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydatautils import dt_summary, last_n_days
from op_analytics.coreutils.time import now_dt
from op_analytics.coreutils.env.vault import env_get_or_none
from datetime import datetime, timedelta
from op_analytics.coreutils.prometheus.client import PrometheusClient

from .dataaccess import PlatformMetrics

log = structlog.get_logger()

username = env_get_or_none("PROMETHEUS_ENG_USERNAME")
password = env_get_or_none("PROMETHEUS_ENG_PASSWORD")
base_url = env_get_or_none("PROMETHEUS_ENG_BASE_URL")


@dataclass
class PrometheusDailyPull:
    """Daily pull of platform metrics."""

    metrics_df: pl.DataFrame

    @classmethod
    def fetch(cls):
        """Fetch Platform metrics daily datasets."""

        client = PrometheusClient(username=username, password=password, base_url=base_url)

        end_time = datetime.now()
        start_time = end_time - timedelta(
            days=30
        )  # Should it be hard coded to start of data creation 2025-05-07?

        range_result = client.query_range(
            query="sum(increase(nat_tests_total[1h]))", start=start_time, end=end_time, step="1d"
        )

        # Extract the list of values (timestamp and value pairs)
        values = range_result["data"]["result"][0]["values"]
        df = pd.DataFrame(values, columns=["unix_time", "value"])

        df["metric"] = range_result["data"]["result"][0]["metric"]
        df["datetime"] = pd.to_datetime(df["unix_time"], unit="s")
        df["value"] = df["value"].astype(float)

        df.rename(columns={"value": "total_new_nat_tests"}, inplace=True)

        current_dt: str = now_dt()

        metrics_raw_data = df[["datetime", "metric", "unix_time", "total_new_nat_tests"]]
        metrics_df = pl.DataFrame(
            metrics_raw_data,
            schema={
                "datetime": pl.Date(),
                "metric": pl.String,
                "unix_time": pl.Int64,
                "total_new_nat_tests": pl.Float64,
            },
        ).rename({"datetime": "dt"})

        metrics_df_truncated = last_n_days(
            metrics_df,
            n_dates=7,
            reference_dt=current_dt,
            date_column_type_is_str=True,
        )

        return PrometheusDailyPull(
            # Use the full dataframe when backfilling:
            metrics_df=metrics_df,
            # Use truncated dataframe when running daily:
            # metrics_df=metrics_df_truncated,
        )
