from dataclasses import dataclass
from op_coreutils.logger import structlog

from op_datasets.pipeline.ozone import BlockBatch
from op_datasets.schemas import CoreDataset

from overrides import EnforceOverrides, override


log = structlog.get_logger()


class CoreDatasetSource(EnforceOverrides):
    @classmethod
    def from_spec(cls, source_spec: str) -> "CoreDatasetSource":
        if source_spec.startswith("goldsky"):
            return GoldskySource()

        if source_spec.startswith("file://"):
            return LocalFileSource(basepath=source_spec.removeprefix("file://"))

        raise NotImplementedError()

    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        raise NotImplementedError()


@dataclass
class GoldskySource(CoreDatasetSource):
    @override
    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        from op_datasets.coretables import fromgoldsky

        return fromgoldsky.read_core_tables(datasets, block_batch)


@dataclass
class LocalFileSource(CoreDatasetSource):
    basepath: str

    @override
    def read_from_source(
        self,
        datasets: dict[str, CoreDataset],
        block_batch: BlockBatch,
    ):
        from op_datasets.coretables import fromlocal

        return fromlocal.read_core_tables(self.basepath, datasets, block_batch)
