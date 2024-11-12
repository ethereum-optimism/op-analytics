from enum import Enum
from .superchain_raw import load_superchain_raw_to_bq

from op_coreutils.partitioned import DataLocation


class PipelineStage(str, Enum):
    """Supported storage locations for partitioned data."""

    RAW_ONCHAIN = "RAW_ONCHAIN"
    INTERMEDIATE_MODEL = "INTERMEDIATE_MODEL"


def load_to_bq(
    stage: PipelineStage,
    location: DataLocation,
    range_spec: str,
    dryrun: bool,
    force: bool,
):
    if stage == PipelineStage.RAW_ONCHAIN:
        return load_superchain_raw_to_bq(
            location=location,
            range_spec=range_spec,
            dryrun=dryrun,
            force=force,
        )

    raise NotImplementedError()
