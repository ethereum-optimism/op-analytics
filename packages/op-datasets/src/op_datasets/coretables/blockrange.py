from dataclasses import dataclass

import re

MIN_MAX_RE = re.compile(r"^(?P<min>\d+):(?P<max>\d+)$")


@dataclass
class BlockRange:
    min: int  # inclusive
    max: int  # inclusive

    @classmethod
    def from_str(cls, block_range_spec: str) -> "BlockRange":
        if minmax := MIN_MAX_RE.fullmatch(block_range_spec):
            return cls(minmax.groupdict()["min"], minmax.groupdict()["max"])

        raise NotImplementedError()

    def filter(self, number_column: str = "number"):
        return f" {number_column} >= {self.min} and {number_column} <= {self.max}"
