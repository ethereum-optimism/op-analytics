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

DELEGATES_SCHEMA = pl.Schema(
    [
        ("delegate", pl.String),
        ("num_of_delegators", pl.Float64),
        ("direct_vp", pl.Float64),
        ("advanced_vp", pl.Float64),
        ("voting_power", pl.Float64),
        ("contract", pl.String),
    ]
)

PROPOSALS_SCHEMA = pl.Schema(
    [
        ("proposal_id", pl.Float64),
        ("contract", pl.String),
        ("proposer", pl.String),
        ("description", pl.String),
        ("ordinal", pl.Int64),
        ("created_block", pl.Int64),
        ("start_block", pl.Int64),
        ("end_block", pl.Int64),
        ("queued_block", pl.Float64),
        ("cancelled_block", pl.Float64),
        ("executed_block", pl.Float64),
        ("proposal_data", pl.String),
        ("proposal_data_raw", pl.String),
        ("proposal_type", pl.String),
        ("proposal_type_data", pl.String),
        ("proposal_results", pl.String),
        ("created_transaction_hash", pl.String),
        ("cancelled_transaction_hash", pl.String),
        ("queued_transaction_hash", pl.String),
        ("executed_transaction_hash", pl.String),
    ]
)

VOTES_SCHEMA = pl.Schema(
    [
        ("transaction_hash", pl.String),
        ("proposal_id", pl.Float64),
        ("voter", pl.String),
        ("support", pl.Int64),
        ("weight", pl.Float64),
        ("reason", pl.String),
        ("block_number", pl.Int64),
        ("params", pl.String),
        ("start_block", pl.Int64),
        ("description", pl.String),
        ("proposal_data", pl.String),
        ("proposal_type", pl.String),
        ("contract", pl.String),
        ("chain_id", pl.Int64),
    ]
)


AGORA_PUBLIC_BUCKET_DATA = {
    "delegate_changed_events": (Agora.DELEGATE_CHANGED_EVENTS, DELEGATE_CHANGED_EVENTS_SCHEMA),
    "delegates": (Agora.DELEGATES, DELEGATES_SCHEMA),
    "proposals_v2": (Agora.PROPOSALS, PROPOSALS_SCHEMA),
    "votes": (Agora.VOTES, VOTES_SCHEMA),
}


def execute_pull():
    result = {}

    for path, (table, schema) in AGORA_PUBLIC_BUCKET_DATA.items():
        full_path = f"{PREFIX}/{path}"

        with bound_contextvars(gcs_path=full_path):
            # Pull.
            log.info("reading csv")
            df = pl.read_csv(full_path, schema=schema, n_threads=1, low_memory=True)

            # Every time we pull data we are fetching the complete dataset. So
            # we store it under a fixed date partition in GCS. This way we only
            # ever keep the latest copy that was fetched.
            table.write(dataframe=df.with_columns(dt=pl.lit(DEFAULT_DT)))
            log.info(f"wrote {len(df)} to {table.value}")

            # Track in results.
            result[path] = len(df)

    return result
