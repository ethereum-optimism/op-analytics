from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class ContractLabels(DailyDataset):
    """Various sources of truth for contract labels."""

    MANUAL = "manual_v1"
    OLI = "oli_v1"
    OPATLAS = "opatlas_v1"
    GROWTHEPIE = "growthepie_v1"
