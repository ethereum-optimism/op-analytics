from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import DailyDataset

log = structlog.get_logger()


class Agora(DailyDataset):
    """Read data from agora.

    For now we are only reading from the public GCS bucket.
    Possibly more to come later on.
    """

    DELEGATE_CHANGED_EVENTS = "delegate_changed_events_v1"
