from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class ChainsMeta(DailyDataset):
    """Chain metadata related tables."""

    RAW_GSHEET = "raw_gsheet_v1"
