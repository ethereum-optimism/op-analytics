from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class L2Beat(DailyDataset):
    """Supported l2beat datasets."""

    # Metadata for L2Beat projects.
    CHAIN_SUMMARY = "projects_v1"

    # Project activity
    ACTIVITY = "activity_v1"

    # Project TVL
    TVL = "tvl_v1"

    # TVS breakdown
    TVS_BREAKDOWN = "tvsbreakdown_v1"
