from enum import Enum
import polars as pl

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.partitioned.dailydata import read_daily_data, write_daily_data
from op_analytics.coreutils.partitioned.location import DataLocation

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

    def read(
        self,
        min_date: str | None = None,
        max_date: str | None = None,
        date_range_spec: str | None = None,
    ) -> str:
        """Read Agora data. Optionally filtered by date.

        This uses the same approach as defillama/dataaccess.py for consistency.
        """
        log.info(
            f"Reading dataset {self.value!r} from {self.root_path!r} "
            f"with filters min_date={min_date}, max_date={max_date}, date_range_spec={date_range_spec}"
        )
        return read_daily_data(
            root_path=self.root_path,
            min_date=min_date,
            max_date=max_date,
            date_range_spec=date_range_spec,
            location=DataLocation.LOCAL,
        )
