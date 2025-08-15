from op_analytics.dataengine.core.di.component_registry import ComponentRegistry
from op_analytics.dataengine.marker.memory_marker_store import MemoryMarkerStore
from op_analytics.dataengine.catalog.memory_dataset_catalog import MemoryDatasetCatalog
from op_analytics.dataengine.core.definitions.step_ref import StepRef
from op_analytics.dataengine.core.types.partition import Partition
from op_analytics.dataengine.core.types.product_ref import ProductRef

# Demo provider source & steps
from op_analytics.dataengine.core.interfaces.source import ISource
from op_analytics.dataengine.steps.ingest_source_step import IngestSourceStep
from op_analytics.dataengine.steps.multiply_transform_step import MultiplyTransformStep
from op_analytics.dataengine.steps.clickhouse_sink_step import ClickHouseSinkStep
from op_analytics.dataengine.steps.catalog_source_step import CatalogSourceStep

class DummySource(ISource[int]):
    def __init__(self, data: list[int]):
        self._data = data
    def fetch(self, partition):
        return list(self._data)

# 1) Build & run the producer pipeline (ingest → multiply → sink/publish)
producer = (
    ComponentRegistry()
        .with_marker_store(MemoryMarkerStore())
        .with_catalog(MemoryDatasetCatalog())
        .with_source_cls('dummy', DummySource, data=[1, 2, 3])
        .with_step_cls('ingest', IngestSourceStep)
        .with_step_cls('multiply', MultiplyTransformStep, factor=10)
        .with_step_cls('ch_sink', ClickHouseSinkStep, table='db.tbl', dataset_name='blocks_with_fees', dataset_version='v1')
        ).create_pipeline(
            name='producer',
            steps=[
                StepRef(name='ingest',  bindings={'source': 'dummy'}),
                StepRef(name='multiply'),
                StepRef(name='ch_sink'),
            ]
    )

producer.run(Partition({"dt": "2025-08-12"}))

# 2) Build a consumer pipeline that waits on the product and reads via catalog
product = ProductRef(name='blocks_with_fees', version='v1', storage_kind='clickhouse', locator={'table': 'db.tbl'})

# In a real provider you’d construct a source factory that reads from ClickHouse table in `locator`
from functools import partial
source_factory = partial(DummySource, data=[10,20,30])  # stand‑in for demo

consumer = (ComponentRegistry()
    .with_marker_store(MemoryMarkerStore())
    .with_catalog(MemoryDatasetCatalog())
    .with_step_cls('catalog_source', CatalogSourceStep, catalog=MemoryDatasetCatalog(), product=product, source_factory=source_factory)
    ).create_pipeline(
        name='consumer',
        steps=[StepRef(name='catalog_source')]
    )

out = consumer.run(Partition({"dt": "2025-08-12"}))
print(out.rows)  # would be data read from the published product
