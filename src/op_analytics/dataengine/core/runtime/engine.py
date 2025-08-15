from typing import Dict

from fastcore.transform import Pipeline

from op_analytics.dataengine.core.definitions.pipeline_definition import PipelineDefinition
from op_analytics.dataengine.core.di.class_provider import ClassProvider
from op_analytics.dataengine.core.interfaces.dataset_catalog import DatasetCatalog
from op_analytics.dataengine.core.interfaces.marker_store import MarkerStore
from op_analytics.dataengine.core.interfaces.source import ISource
from op_analytics.dataengine.core.interfaces.step import Step


class DataEngine:
    """Resolves registered providers and constructs pipelines bound to MarkerStore/Catalog."""
    def __init__(
        self,
        *,
        sources: Dict[str, ClassProvider[ISource[object]]],
        steps: Dict[str, ClassProvider[Step[object, object]]],
        marker_store: MarkerStore,
        catalog: DatasetCatalog | None = None
    ) -> None:
        self.sources = sources
        self.steps = steps
        self.marker_store = marker_store
        self.catalog = catalog

    def make_pipeline(self, defn: PipelineDefinition) -> Pipeline:
        return Pipeline(defn, self)
