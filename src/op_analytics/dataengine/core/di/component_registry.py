from typing import Dict, List, Type

from op_analytics.dataengine.core.definitions.pipeline_definition import PipelineDefinition
from op_analytics.dataengine.core.definitions.step_definition import StepDefinition
from op_analytics.dataengine.core.definitions.step_ref import StepRef
from op_analytics.dataengine.core.di.class_provider import ClassProvider
from op_analytics.dataengine.core.interfaces.dataset_catalog import DatasetCatalog
from op_analytics.dataengine.core.interfaces.marker_store import MarkerStore
from op_analytics.dataengine.core.interfaces.source import ISource
from op_analytics.dataengine.core.interfaces.step import Step
from op_analytics.dataengine.core.runtime.engine import DataEngine


class ComponentRegistry:
    """Registers classes and builds a DataEngine or Pipeline from step refs."""
    def __init__(self) -> None:
        self._source_providers: Dict[str, ClassProvider[ISource[object]]] = {}
        self._step_providers: Dict[str, ClassProvider[Step[object, object]]] = {}
        self._marker_store: MarkerStore | None = None
        self._catalog: DatasetCatalog | None = None

    def with_source_cls(self, name: str, cls: Type[ISource[object]], **kwargs: object) -> 'ComponentRegistry':
        self._source_providers[name] = ClassProvider(cls=cls, kwargs=dict(kwargs))
        return self

    def with_step_cls(self, name: str, cls: Type[Step[object, object]], **kwargs: object) -> 'ComponentRegistry':
        self._step_providers[name] = ClassProvider(cls=cls, kwargs=dict(kwargs))
        return self

    def with_marker_store(self, store: MarkerStore) -> 'ComponentRegistry':
        self._marker_store = store
        return self

    def with_catalog(self, catalog: DatasetCatalog) -> 'ComponentRegistry':
        self._catalog = catalog
        return self

    def create_engine(self) -> DataEngine:
        if not self._marker_store:
            raise RuntimeError("MarkerStore required")
        return DataEngine(
            sources=self._source_providers,
            steps=self._step_providers,
            marker_store=self._marker_store,
            catalog=self._catalog,
        )

    def create_pipeline(self, name: str, steps: List[StepRef]):
        defns = [
            StepDefinition(
                step_id=(ref.step_id or ref.name),
                implementation_name=ref.name,
                config=ref.config,
                bindings=ref.bindings,
            ) for ref in steps
        ]
        return self.create_engine().make_pipeline(PipelineDefinition(name=name, steps=defns))
