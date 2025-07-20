from dagster import (
    OpExecutionContext,
    asset,
    Field,
)

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.orchestrate import normalize_blockbatch_models, normalize_chains
from op_analytics.dagster.utils.jobs import get_logs_url


@asset(config_schema={"range_spec": Field(str, default_value="m24hours")})
def update_a(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")
    context.log.info(context.op_config.get("range_spec"))

    excluded_chains = {"celo"}
    chains = [chain for chain in normalize_chains("ALL") if chain not in excluded_chains]

    result = compute_blockbatch(
        chains=chains,
        models=normalize_blockbatch_models("GROUPA"),
        range_spec=context.op_config.get("range_spec"),
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)


@asset(config_schema={"range_spec": Field(str, default_value="m24hours")})
def update_b(context: OpExecutionContext):
    context.log.info(f"LOGS URL: {get_logs_url()}")
    context.log.info(context.op_config.get("range_spec"))

    excluded_chains = {"celo", "ethereum"}
    chains = [chain for chain in normalize_chains("ALL") if chain not in excluded_chains]

    result = compute_blockbatch(
        chains=chains,
        models=normalize_blockbatch_models("GROUPB"),
        range_spec=context.op_config.get("range_spec"),
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
