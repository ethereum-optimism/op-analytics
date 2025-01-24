import importlib
from dataclasses import dataclass
from threading import Lock
from typing import ClassVar, Callable, Protocol


from op_analytics.coreutils.duckdb_inmem.client import DuckDBContext, ParquetData

from .auxview import AuxiliaryTemplate
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
    _registry: ClassVar[dict[ModelPath, "PythonModel"]] = {}

    # Thread-safe access to the registry.
    _registry_lock = Lock()

    path: ModelPath
    input_datasets: list[str]
    expected_output_datasets: list[str]
    auxiliary_templates: list[str]
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
        """Load a model based on its full function path.

        The python module where the module is declared is imported, which results
        in the model included in the PythonModel registry.
        """

        # Do not support models defined on arbitary python modules.
        if "." in full_function_path:
            raise ValueError(f"error loading model: invalid path: {full_function_path}")

        module = full_function_path
        function_name = full_function_path

        model_path = ModelPath(module, function_name)

        with cls._registry_lock:
            current_models = set(cls._registry.keys())

            if model_path not in cls._registry:
                # Import the module so that the model gets registered.
                # For now the module and model name are the same by convention, but
                # that can change if we need to.
                load_path = f"op_analytics.datapipeline.models.code.{module}"
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

                key_loaded = list(keys_loaded)[0]

                # Add a key alias to the registry. This is to support models defined in a directory
                # using a model.py file: <MODEL_NAME>/model.py.
                submodule_key = ModelPath(
                    module=f"{function_name}.model",
                    function_name=function_name,
                )

                key_alias = ModelPath(
                    module=function_name,
                    function_name=function_name,
                )

                if key_loaded == submodule_key:
                    cls._registry[key_alias] = cls._registry[key_loaded]

        return cls._registry[model_path]

    @property
    def func(self) -> ModelFunction:
        return self.model_func
