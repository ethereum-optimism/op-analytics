from dagster import (
    OpExecutionContext,
    asset,
    Config,
)
from typing import Optional

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.orchestrate import normalize_blockbatch_models, normalize_chains
from op_analytics.dagster.utils.jobs import get_logs_url


class UpdateAConfig(Config):
    range_spec: Optional[str] = None  # Default is None


@asset
def update_a(context: OpExecutionContext, config: UpdateAConfig):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    result = compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("GROUPA"),
        range_spec=config.range_spec or "m24hours",
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)


class UpdateBConfig(Config):
    range_spec: Optional[str] = None  # Default is None


@asset
def update_b(context: OpExecutionContext, config: UpdateBConfig):
    context.log.info(f"LOGS URL: {get_logs_url()}")

    result = compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("GROUPB"),
        range_spec=config.range_spec or "m24hours",
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
