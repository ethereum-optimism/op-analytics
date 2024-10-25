import os
from dataclasses import dataclass
from datetime import date

from op_coreutils.time import date_fromstr


from .types import SinkOutputRootPath


@dataclass
class KeyValue:
    key: str
    value: str


@dataclass
class WrittenParquetPath:
    """Represent a single object written to storage in a partitioned path."""

    root: SinkOutputRootPath
    basename: str
    partitions: list[KeyValue]
    row_count: int

    @classmethod
    def from_partition(
        cls, root: SinkOutputRootPath, basename: str, partitions: list[KeyValue], row_count: int
    ) -> "WrittenParquetPath":
        return cls(
            root=root,
            basename=basename,
            partitions=partitions,
            row_count=row_count,
        )

    @property
    def partitions_path(self):
        return "/".join(f"{col.key}={col.value}" for col in self.partitions)

    @property
    def full_path(self):
        return os.path.join(self.root, self.partitions_path, self.basename)

    def dt_value(self) -> date:
        """Return the "dt" value if this partition has a "dt" column."""
        for col in self.partitions:
            if col.key == "dt":
                if isinstance(col.value, str):
                    return date_fromstr(col.value)
                else:
                    raise ValueError(
                        f"a string value is expected on the 'dt' partition column: got dt={col.value}"
                    )
        raise ValueError(f"partition does not have a 'dt' column: {self}")

    def safe_dt_value(self) -> date:
        try:
            return self.dt_value()
        except ValueError:
            return date_fromstr("1970-01-01")
