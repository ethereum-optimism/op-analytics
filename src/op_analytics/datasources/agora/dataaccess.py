from enum import Enum
import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import write_daily_data

log = structlog.get_logger()


class Agora(str, Enum):
    """Supported agora datasets."""

    DELEGATES = "delegates_v1"
    DELEGATE_VOTES = "delegate_votes_v1"
    PROPOSALS = "proposals_v1"
    DELEGATORS = "delegators_v1"
    DELEGATEES = "delegatees_v1"

    @property
    def root_path(self) -> str:
        return f"agora/{self.value}"

    def write(
        self,
        dataframe: pl.DataFrame,
        sort_by: list[str] | None = None,
        force_complete: bool = False,
    ):
        """Write Agora dataset using daily partitioning."""
        if dataframe.height == 0:
            log.warning(f"Skipping write of empty dataframe for {self.value!r}")
            return

        log.info(f"Writing dataset {self.value!r} to {self.root_path!r}")
        return write_daily_data(
            root_path=self.root_path,
            dataframe=dataframe,
            sort_by=sort_by,
        )
