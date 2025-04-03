import importlib
from dataclasses import dataclass
from threading import Lock
from typing import ClassVar, Callable, Protocol


from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData

from .auxtemplate import AuxiliaryTemplate
from .types import NamedRelations


class ModelDataReader(Protocol):
    def remote_parquet(
        self,
        dataset: str,
        first_n_parquet_files: int | None = None,
    ) -> ParquetData:
        """Return a remote parquet data object for the given dataset."""
        ...


type ModelFunction = Callable[
    [DuckDBContext, dict[str, ParquetData], dict[str, AuxiliaryTemplate]], NamedRelations
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
    # The registry stores all instances of PythonModel.
    _registry: ClassVar[dict[str, "PythonModel"]] = {}

    # Thread-safe access to the registry.
    _registry_lock = Lock()

    path: ModelPath
    input_datasets: list[str]
    expected_output_datasets: list[str]
    auxiliary_templates: list[str]
    model_func: ModelFunction
    excluded_chains: list[str]

    def __post_init__(self):
        self._registry[self.path.function_name] = self

    @property
    def name(self):
        return self.path.function_name

    @property
    def fq_model_path(self):
        """Fully qualified path that will be used to store the model in GCS."""
        return self.path.fq_model_path

    @classmethod
    def get(cls, function_name: str) -> "PythonModel":
        """Load a model.

        The python module where the model is defined is imported, which results
        in the model included in the PythonModel registry.
        """

        if function_name in cls._registry:
            return cls._registry[function_name]

        if "." in function_name:
            raise ValueError(f"error loading model: invalid path: {function_name}")

        with cls._registry_lock:
            current_models = set(cls._registry.keys())

            # Import the module so that the model gets registered.
            # The module and function are the same by convention.
            load_path = f"op_analytics.datapipeline.models.code.{function_name}"
            importlib.import_module(load_path)

            loaded_models = set(cls._registry.keys())

            num_loaded = len(loaded_models) - len(current_models)
            if num_loaded == 0:
                raise Exception(
                    f"error loading model: no modules were loaded from path: {load_path}"
                )

            keys_loaded = loaded_models - current_models
            if num_loaded > 1:
                raise Exception(
                    f"error loading model: loaded more than one module from path: {load_path}: {keys_loaded}"
                )

        return cls._registry[function_name]

    @property
    def func(self) -> ModelFunction:
        return self.model_func
