from typing import Generic, List, TypeVar, Callable
from ..interfaces.step import Step
from ..interfaces.source import ISource
from ..interfaces.dataset_catalog import DatasetCatalog
from ..types.product_ref import ProductRef
from ..types.dataset import Dataset
from ..types.run_context import RunContext

T = TypeVar("T")

class CatalogSourceStep(Step[object, T], Generic[T]):
    """Source step that resolves a ProductRef via the catalog and reads it through a provider source."""
    def __init__(self, catalog: DatasetCatalog, product: ProductRef, source_factory: Callable[[], ISource[T]], version: str = "v1") -> None:
        self._catalog = catalog
        self._product = product
        self._source_factory = source_factory
        self._version = version

    def run(self, data: Dataset[object], ctx: RunContext) -> Dataset[T]:
        resolved = self._catalog.resolve(self._product)
        # provider-specific: here we assume source_factory knows how to read from resolved locator
        src = self._source_factory()
        rows: List[T] = list(src.fetch(ctx.partition))
        return Dataset(rows=rows, schema=resolved.name, schema_version=self._version)
