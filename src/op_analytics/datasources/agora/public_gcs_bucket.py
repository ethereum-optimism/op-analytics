import polars as pl

from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT
from op_analytics.coreutils.logger import structlog, bound_contextvars

from .dataaccess import Agora

log = structlog.get_logger()

PREFIX = "gs://agora-optimism-public-usw1/v1/snapshot"

DELEGATE_CHANGED_EVENTS_SCHEMA = pl.Schema(
    [
        ("chain_id", pl.Int64),
        ("address", pl.String),
        ("block_number", pl.Int64),
        ("block_hash", pl.String),
        ("log_index", pl.Int64),
        ("transaction_index", pl.Int64),
        ("transaction_hash", pl.String),
        ("delegator", pl.String),
        ("from_delegate", pl.String),
        ("to_delegate", pl.String),
    ]
)

TABLES = {
    "delegate_changed_events": (Agora.DELEGATE_CHANGED_EVENTS, DELEGATE_CHANGED_EVENTS_SCHEMA)
}


def execute_pull():
    result = {}

    for path, (dataaccess, schema) in TABLES.items():
        full_path = f"{PREFIX}/{path}"

        with bound_contextvars(gcs_path=full_path):
            # Pull.
            log.info("reading csv")
            df = pl.read_csv(full_path, schema=schema, n_threads=1, low_memory=True)

            # Every time we pull data we are fetching the complete dataset. So
            # we store it under a fixed date partition in GCS. This way we only
            # ever keep the latest copy that was fetched.
            dataaccess.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))
            log.info(f"wrote {len(df)} to {dataaccess.value}")

            # Track in results.
            result[path] = len(df)

    return result
