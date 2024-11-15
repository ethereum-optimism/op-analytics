from dataclasses import dataclass
from datetime import date, timedelta

from op_coreutils.time import date_toepoch, now_date

import re

MIN_MAX_RE = re.compile(r"^@(?P<min>\d{8}):(?P<max>\d{8})$")

PLUS_RE = re.compile(r"^@(?P<min>\d{8}):\+(?P<plus>\d+)$")

MDAYS_RE = re.compile(r"^m(?P<num>\d+)days")


@dataclass
class DateRange:
    min: date  # inclusive
    max: date  # exclusive

    max_requested_timestamp: int | None

    def __len__(self):
        diff = self.max - self.min
        return diff.days

    @property
    def min_ts(self) -> int:
        return date_toepoch(self.min)

    @property
    def max_ts(self) -> int:
        return date_toepoch(self.max)

    @property
    def dates(self) -> list[date]:
        result = []
        current = self.min
        while current < self.max:
            result.append(current)
            current += timedelta(days=1)
        return result

    def padded_dates(self) -> list[date]:
        result = [self.min - timedelta(days=1)]
        result.extend(self.dates)
        result.append(self.max)
        return result

    @classmethod
    def from_spec(cls, date_range_spec: str) -> "DateRange":
        if minmax := MIN_MAX_RE.fullmatch(date_range_spec):
            min_str = minmax.groupdict()["min"]
            max_str = minmax.groupdict()["max"]
            max_date = date.fromisoformat(max_str)
            return cls(
                min=date.fromisoformat(min_str),
                max=max_date,
                max_requested_timestamp=date_toepoch(max_date),
            )

        if plusstr := PLUS_RE.fullmatch(date_range_spec):
            min_val = date.fromisoformat(plusstr.groupdict()["min"])
            plus_val = int(plusstr.groupdict()["plus"])

            max_date = min_val + timedelta(days=plus_val)
            return cls(
                min=min_val,
                max=max_date,
                max_requested_timestamp=date_toepoch(max_date),
            )

        if mdaysstr := MDAYS_RE.fullmatch(date_range_spec):
            num_days = int(mdaysstr.groupdict()["num"])

            # NOTE: Remember that max is exclusive.
            max_val = now_date() + timedelta(days=1)

            return cls(
                min=max_val - timedelta(days=num_days),
                max=max_val,
                max_requested_timestamp=None,  # the max was not explicitly requested
            )

        raise NotImplementedError()
