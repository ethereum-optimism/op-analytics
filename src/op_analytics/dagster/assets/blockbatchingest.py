from dagster import (
    OpExecutionContext,
    asset, Field,
)

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.orchestrate import normalize_chains
from op_analytics.dagster.utils.jobs import get_logs_url
from op_analytics.datapipeline.etl.ingestion.main import ingest
from op_analytics.datapipeline.etl.ingestion.sources import RawOnchainDataProvider


@asset(
    config_schema={
        "chains": Field(str, default_value="ALL"),
        "range_spec": Field(str, default_value="m32hours")
    }
)
def audit_and_ingest(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    chains = context.op_config.get("chains")
    range_spec = context.op_config.get("range_spec")

    context.log.info(chains)
    context.log.info(range_spec)

    result = ingest(
        chains=normalize_chains(chains),
        range_spec=range_spec,
        read_from=RawOnchainDataProvider.GOLDSKY,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
