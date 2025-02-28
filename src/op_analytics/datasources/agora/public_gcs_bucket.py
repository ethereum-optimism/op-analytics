import duckdb
import polars as pl

from op_analytics.coreutils.partitioned.dailydata import DEFAULT_DT, PARQUET_FILENAME
from op_analytics.coreutils.logger import structlog, bound_contextvars
from op_analytics.coreutils.duckdb_inmem.client import init_client
from op_analytics.coreutils.partitioned.dailydatawrite import construct_marker, write_markers


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
        ("num_of_delegators", pl.Int64),
        ("direct_vp", pl.String),
        ("advanced_vp", pl.String),
        ("voting_power", pl.String),
        ("contract", pl.String),
    ]
)


PROPOSALS_SCHEMA = pl.Schema(
    [
        ("proposal_id", pl.String),
        ("contract", pl.String),
        ("proposer", pl.String),
        ("description", pl.String),
        ("ordinal", pl.Int64),
        ("created_block", pl.Int64),
        ("start_block", pl.Int64),
        ("end_block", pl.Int64),
        ("queued_block", pl.Int64),
        ("cancelled_block", pl.Int64),
        ("executed_block", pl.Int64),
        ("proposal_data", pl.String),
        ("proposal_data_raw", pl.String),
        ("proposal_type", pl.String),
        ("proposal_type_data", pl.String),
        ("proposal_results", pl.String),
        ("created_transaction_hash", pl.String),
        ("cancelled_transaction_hash", pl.String),
        ("queued_transaction_hash", pl.String),
        ("executed_transaction_hash", pl.String),
        ("proposal_type_id", pl.Int64),
    ]
)

VOTES_SCHEMA = pl.Schema(
    [
        ("transaction_hash", pl.String),
        ("proposal_id", pl.String),
        ("voter", pl.String),
        ("support", pl.Int64),
        ("weight", pl.String),
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

VOTING_POWER_SNAPS_SCHEMA = pl.Schema(
    [
        ("id", pl.String),
        ("delegate", pl.String),
        ("balance", pl.String),
        ("block_number", pl.Int64),
        ("ordinal", pl.Int64),
        ("transaction_index", pl.Int64),
        ("log_index", pl.Int64),
        ("contract", pl.String),
    ]
)


AGORA_PUBLIC_BUCKET_DATA = {
    "delegate_changed_events": (Agora.DELEGATE_CHANGED_EVENTS, DELEGATE_CHANGED_EVENTS_SCHEMA),
    "delegates": (Agora.DELEGATES, DELEGATES_SCHEMA),
    "proposals_v2": (Agora.PROPOSALS, PROPOSALS_SCHEMA),
    "votes": (Agora.VOTES, VOTES_SCHEMA),
    "voting_power_snaps": (Agora.VOTING_POWER_SNAPS, VOTING_POWER_SNAPS_SCHEMA),
}


def execute_pull():
    ctx = init_client()

    result = {}
    for path, (table, schema) in AGORA_PUBLIC_BUCKET_DATA.items():
        full_path = f"{PREFIX}/{path}"

        with bound_contextvars(gcs_path=full_path):
            # Pull.
            log.info("reading csv")

            # Every time we pull data we are fetching the complete dataset. So
            # we store it under a fixed date partition in GCS. This way we only
            # ever keep the latest copy that was fetched.
            output_path = (
                f"gs://oplabs-tools-data-sink/{table.root_path}/dt={DEFAULT_DT}/{PARQUET_FILENAME}"
            )

            try:
                # Use duckdb to stream-write the CSV over to a parquet file in GCS.
                ctx.client.read_csv(
                    full_path,
                    columns=duckdb_schema(schema),
                ).write_parquet(output_path)
            except duckdb.InvalidInputException as ex:
                # An error here is most likely due to changes in the Agora CSV schema that do
                # not match our expected schema. We re-raise the exception adding information
                # about the schema currently in GCS to help us debug.

                rel = ctx.client.read_csv(full_path).limit(100)
                detected_schema = dict(zip(rel.columns, rel.dtypes))

                raise Exception(f"{detected_schema=}") from ex

            # Read back the data to verify row count and write the marker.
            row_count = len(ctx.client.read_parquet(output_path))
            log.info(f"verified parquet data in GCS with {row_count} rows")
            marker = construct_marker(
                root_path=table.root_path,
                datestr=DEFAULT_DT,
                row_count=row_count,
                process_name="from_duckdb",
            )
            write_markers(markers_arrow=marker)
            result[table.table] = row_count

    return result


def duckdb_type(val):
    if val == pl.String:
        return "VARCHAR"

    if val == pl.Int64:
        return "INT64"

    raise NotImplementedError()


def duckdb_schema(polars_schema: pl.Schema) -> dict[str, str]:
    return {name: duckdb_type(val) for name, val in polars_schema.items()}
