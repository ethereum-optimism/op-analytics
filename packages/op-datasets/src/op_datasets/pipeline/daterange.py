from dataclasses import dataclass
from datetime import date, timedelta

from op_coreutils.time import date_toepoch

import re

MIN_MAX_RE = re.compile(r"^(?P<min>\d{8}):(?P<max>\d{8})$")

PLUS_RE = re.compile(r"^(?P<min>\d{8}):\+(?P<plus>\d+)$")


@dataclass
class DateRange:
    min: date  # inclusive
    max: date  # exclusive

    def __len__(self):
        diff = self.max - self.min
        return diff.days

    @property
    def min_ts(self) -> int:
        return date_toepoch(self.min)

    @property
    def max_ts(self) -> int:
        return date_toepoch(self.max)

    @classmethod
    def from_spec(cls, date_range_spec: str) -> "DateRange":
        if minmax := MIN_MAX_RE.fullmatch(date_range_spec):
            min_str = minmax.groupdict()["min"]
            max_str = minmax.groupdict()["max"]
            return cls(date.fromisoformat(min_str), date.fromisoformat(max_str))

        if plusstr := PLUS_RE.fullmatch(date_range_spec):
            min_val = date.fromisoformat(plusstr.groupdict()["min"])
            plus_val = int(plusstr.groupdict()["plus"])
            return cls(min_val, min_val + timedelta(days=plus_val))

        raise NotImplementedError()
