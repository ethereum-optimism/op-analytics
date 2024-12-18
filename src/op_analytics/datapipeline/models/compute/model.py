import importlib
from dataclasses import dataclass
from typing import ClassVar, Callable, Protocol


from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext

from .auxview import AuxiliaryView
from .types import NamedRelations


class ParquetData(Protocol):
    def create_table(self) -> str: ...
    def create_view(self) -> str: ...


class ModelDataReader(Protocol):
    def remote_parquet(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> ParquetData:
        """Return a remote parquet data object for the given dataset."""
        ...


type ModelFunction = Callable[
    [DuckDBContext, dict[str, ParquetData], dict[str, AuxiliaryView]], NamedRelations
]


@dataclass(frozen=True)
class ModelPath:
    # Name of the python module where the model function is defined.
    # The name is relative to the code/ directory.
    module: str

    # Name of the python function that defines the model.
    function_name: str

    @property
    def fq_model_path(self):
        """Path where model results will be stored.

        We decided not to include the module in the model path for two reasons.

        1) The module can have '.' in it if it is nested. And table names in
           some engines do not allow dots.

        2) If we move module functions around during a refactor we don't want the
           storage path to change.
        """
        return self.function_name


@dataclass
class PythonModel:
    # The registry stores all instances of PythonModel
    _registry: ClassVar[dict[ModelPath, "PythonModel"]] = {}

    path: ModelPath
    input_datasets: list[str]
    expected_output_datasets: list[str]
    auxiliary_views: list[str]
    model_func: ModelFunction

    def __post_init__(self):
        self._registry[self.path] = self

    @property
    def name(self):
        return self.path.function_name

    @property
    def fq_model_path(self):
        """Fully qualified path that will be used to store the model in GCS."""
        return self.path.fq_model_path

    @classmethod
    def get(cls, full_function_path: str) -> "PythonModel":
        if "." not in full_function_path:
            # Support for models where the module and function have the same name.
            module = full_function_path
            function_name = full_function_path
        else:
            module, function_name = full_function_path.rsplit(".", maxsplit=1)

        model_path = ModelPath(module, function_name)

        if model_path not in cls._registry:
            # Import the module so that the model gets registered.
            # For now the module and model name are the same by convention, but
            # that can change if we need to.
            importlib.import_module(f"op_analytics.datapipeline.models.code.{module}")

        return cls._registry[model_path]

    @property
    def func(self) -> ModelFunction:
        return self.model_func

    # def rendered_auxiliary_views(self) -> list[AuxiliaryView]:
    #     if self._rendered_auxiliary_views is None:
    #         result = []
    #         for q in self.auxiliary_views or []:
    #             rendered = q.render()
    #             result.append(
    #                 AuxiliaryView(
    #                     template=q,
    #                     rendered_query=rendered.query,
    #                 )
    #             )
    #         self._rendered_views = result

    #     return self._rendered_views
