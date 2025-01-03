from dataclasses import dataclass
from datetime import datetime, timedelta

from op_analytics.coreutils.time import datetime_toepoch, now, datetime_fromdate

from .daterange import DateRange

import re


MHOURS_RE = re.compile(r"^m(?P<num>\d+)hours")


@dataclass
class TimeRange:
    min: datetime  # inclusive
    max: datetime  # exclusive

    max_requested_timestamp: int | None

    @property
    def min_ts(self) -> int:
        return datetime_toepoch(self.min)

    @property
    def max_ts(self) -> int:
        return datetime_toepoch(self.max)

    @classmethod
    def from_spec(cls, time_range_spec: str) -> "TimeRange":
        try:
            date_range = DateRange.from_spec(time_range_spec)
            return cls(
                min=datetime_fromdate(date_range.min),
                max=datetime_fromdate(date_range.max),
                max_requested_timestamp=date_range.max_requested_timestamp,
            )

        except NotImplementedError:
            if mhoursstr := MHOURS_RE.fullmatch(time_range_spec):
                num_hours = int(mhoursstr.groupdict()["num"])

                max_val = now()

                return cls(
                    min=max_val - timedelta(hours=num_hours),
                    max=max_val,
                    max_requested_timestamp=None,  # the max was not explicitly requested
                )

        raise NotImplementedError()

    def to_date_range(self) -> "DateRange":
        max_date = self.max.date() + timedelta(days=1)

        return DateRange(
            min=self.min.date(),
            max=max_date,
            max_requested_timestamp=datetime_toepoch(datetime_fromdate(max_date)),
        )
