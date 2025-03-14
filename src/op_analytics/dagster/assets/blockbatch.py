from dagster import (
    OpExecutionContext,
    asset,
)

from op_analytics.coreutils.partitioned.location import DataLocation
from op_analytics.datapipeline.etl.blockbatch.main import compute_blockbatch
from op_analytics.datapipeline.orchestrate import normalize_blockbatch_models, normalize_chains


@asset
def update_all(context: OpExecutionContext):
    result = compute_blockbatch(
        chains=normalize_chains("ALL"),
        models=normalize_blockbatch_models("MODELS"),
        range_spec="m24hours",
        read_from=DataLocation.GCS,
        write_to=DataLocation.GCS,
        dryrun=False,
        force_complete=False,
        fork_process=True,
    )
    context.log.info(result)
