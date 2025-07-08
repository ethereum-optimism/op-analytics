from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.orchestrate import normalize_blockbatch_models, normalize_chains
from op_analytics.dagster.utils.jobs import get_logs_url


@asset(config_schema={"range_spec": str})
def update_a(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    result = compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("GROUPA"),
        range_spec=context.op_config.get("range_spec", "m24hours"),
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)


@asset(config_schema={"range_spec": str})
def update_b(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    result = compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("GROUPB"),
        range_spec=context.op_config.get("range_spec", "m24hours"),
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
