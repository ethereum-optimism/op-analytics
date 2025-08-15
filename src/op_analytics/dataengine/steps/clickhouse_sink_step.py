from ..interfaces.step import Step
from ..interfaces.sink import Sink
from ..interfaces.idempotent import Idempotent
from ..interfaces.publishable import Publishable
from ..types.product_ref import ProductRef
from ..types.dataset import Dataset
from ..types.run_context import RunContext

class ClickHouseSinkStep(Step[object, object], Sink[object], Idempotent, Publishable):
    """Sink that writes rows to ClickHouse, publishes a ProductRef, and returns the dataset unchanged."""
    def __init__(self, table: str, dataset_name: str, dataset_version: str, writer_version: str = "v1") -> None:
        self._table = table
        self._dataset_name = dataset_name
        self._dataset_version = dataset_version
        self._writer_version = writer_version

    def product_ref(self) -> ProductRef:
        return ProductRef(
            name=self._dataset_name,
            version=self._dataset_version,
            storage_kind="clickhouse",
            locator={"table": self._table},
        )

    def run(self, data: Dataset[object], ctx: RunContext) -> Dataset[object]:
        self.commit(data, ctx)
        return data

    def commit(self, data: Dataset[object], ctx: RunContext) -> None:
        # TODO: implement ClickHouse write here
        pass

    def fingerprint(self, data: Dataset[object], ctx: RunContext) -> str:
        return f"{ctx.pipeline}|{ctx.step_id}|{self._writer_version}|{self._dataset_name}:{self._dataset_version}"
