import os
from dataclasses import dataclass


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
