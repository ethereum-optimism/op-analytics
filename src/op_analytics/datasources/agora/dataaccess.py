from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Agora(DailyDataset):
    """Read data from agora.

    NOTE: We used to do dailydata ingestion of Agora data, but this was running into
    problems due to the size of the data. For that reason we redid the implementation
    using an incremental approach directly into ClickHouse.
    """

    pass
