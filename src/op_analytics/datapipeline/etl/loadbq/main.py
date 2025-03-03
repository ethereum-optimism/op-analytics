from enum import Enum

from op_analytics.coreutils.partitioned.location import DataLocation

from op_analytics.datapipeline.etl.ingestion.reader.rootpaths import RootPath
from .load import load_blockbatch_to_bq


class PipelineStage(str, Enum):
    """Supported storage locations for partitioned data."""

    RAW_ONCHAIN = "RAW_ONCHAIN"
    INTERMEDIATE_MODEL = "INTERMEDIATE_MODEL"


def load_to_bq(
    stage: PipelineStage,
    location: DataLocation,
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    if stage == PipelineStage.RAW_ONCHAIN:
        return load_superchain_raw_to_bq(
            location=location,
            range_spec=range_spec,
            dryrun=dryrun,
            force_complete=force_complete,
            force_not_ready=force_not_ready,
        )

    raise NotImplementedError()


def load_superchain_raw_to_bq(
    location: DataLocation,
    range_spec: str,
    dryrun: bool,
    force_complete: bool,
    force_not_ready: bool,
):
    return load_blockbatch_to_bq(
        location=location,
        range_spec=range_spec,
        root_paths_to_read=[
            RootPath.of("ingestion/blocks_v1"),
            RootPath.of("ingestion/logs_v1"),
            RootPath.of("ingestion/traces_v1"),
            RootPath.of("ingestion/transactions_v1"),
        ],
        bq_dataset_name="superchain_raw",
        dryrun=dryrun,
        force_complete=force_complete,
        force_not_ready=force_not_ready,
    )
