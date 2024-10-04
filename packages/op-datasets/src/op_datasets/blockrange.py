from dataclasses import dataclass

import re

MIN_MAX_RE = re.compile(r"^(?P<min>\d+):(?P<max>\d+)$")

PLUS_RE = re.compile(r"^(?P<min>\d+):\+(?P<plus>\d+)$")


@dataclass
class BlockRange:
    min: int  # inclusive
    max: int  # exclusive

    def __len__(self):
        return self.max - self.min

    @classmethod
    def from_spec(cls, block_range_spec: str) -> "BlockRange":
        if minmax := MIN_MAX_RE.fullmatch(block_range_spec):
            min_str = minmax.groupdict()["min"]
            max_str = minmax.groupdict()["max"]
            return cls(int(min_str), int(max_str))

        if plusstr := PLUS_RE.fullmatch(block_range_spec):
            min_val = int(plusstr.groupdict()["min"])
            plus_val = int(plusstr.groupdict()["plus"])
            return cls(min_val, min_val + plus_val)

        raise NotImplementedError()

    def filter(self, number_column: str = "number"):
        return f" {number_column} >= {self.min} and {number_column} < {self.max}"
