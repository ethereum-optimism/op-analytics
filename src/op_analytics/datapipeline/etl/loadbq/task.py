from dataclasses import dataclass
from datetime import date

from op_analytics.coreutils.logger import structlog

from .loader import BQLoader, BQOutputData

log = structlog.get_logger()


@dataclass
class DateLoadTask:
    """Task to load all data for a given date to BigQuery."""

    dateval: date
    chains_ready: set[str]
    chains_not_ready: set[str]
    write_manager: BQLoader
    outputs: list[BQOutputData]

    @property
    def contextvars(self):
        return {"date": self.dateval.strftime("%Y-%m-%d")}
