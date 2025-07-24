import abc
from typing import Dict, Optional, Type, List
import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


class BaseChainMetadataLoader(abc.ABC):
    """
    Abstract base class for all chain metadata loaders.
    """

    def __init__(self, **kwargs):
        self.config = kwargs
        log.debug("Initialized loader", loader=self.__class__.__name__, config=kwargs)

    @abc.abstractmethod
    def load_data(self, **kwargs) -> pl.DataFrame:
        """
        Load raw data from the source, map to canonical field names,
        and harmonize to the canonical schema.
        Must be implemented by all loaders.
        """
        pass

    def validate_output(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Validates the output DataFrame.
        Ensures the primary key 'chain_key' exists and is not null.
        """
        if "chain_key" not in df.columns:
            raise ValueError("Output DataFrame must contain a 'chain_key' column.")

        if df.height > 0 and df["chain_key"].is_null().any():
            log.warning(
                "Loader output contains null 'chain_key' values.",
                loader=self.__class__.__name__,
            )
        return df

    def run(self, **kwargs) -> pl.DataFrame:
        """
        Run the loader: load data and validate output.
        """
        log.info("Running loader pipeline", loader=self.__class__.__name__)
        df = self.load_data(**kwargs)
        df = self.validate_output(df)
        return df


class LoaderRegistry:
    """
    Registry for chain metadata loaders by name.
    Allows dynamic discovery and instantiation of loaders.
    """

    _registry: Dict[str, Type[BaseChainMetadataLoader]] = {}

    @classmethod
    def register(cls, name: str, loader_cls: Type[BaseChainMetadataLoader]):
        if name in cls._registry:
            log.warning("Loader already registered, overwriting", name=name)
        cls._registry[name] = loader_cls
        log.info("Registered loader", name=name, loader_cls=loader_cls.__name__)

    @classmethod
    def get_loader(cls, name: str) -> Optional[Type[BaseChainMetadataLoader]]:
        return cls._registry.get(name)

    @classmethod
    def list_loaders(cls) -> List[str]:
        return list(cls._registry.keys())
