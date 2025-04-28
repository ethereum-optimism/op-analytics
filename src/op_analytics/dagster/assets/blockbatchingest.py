from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.orchestrate import normalize_chains
from op_analytics.dagster.utils.jobs import get_logs_url
from op_analytics.datapipeline.etl.ingestion.main import ingest
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider


@asset
def audit_and_ingest(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    result = ingest(
        chains=normalize_chains("ALL"),
        range_spec="m16hours",
        read_from=RawOnchainDataProvider.GOLDSKY,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
