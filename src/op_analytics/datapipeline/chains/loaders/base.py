import abc
from typing import Dict, Optional, Type, List
import polars as pl
from op_analytics.coreutils.logger import structlog

log = structlog.get_logger()


class BaseChainMetadataLoader(abc.ABC):
    """
    Abstract base class for all chain metadata loaders.
    Enforces a canonical schema and validation for downstream aggregation.
    """

    # Required fields for the canonical chain metadata schema.
    # - chain_id: Unique identifier for the chain (int or str, depending on source)
    # - chain_name: Standardized name for the chain (e.g., 'optimism', 'base')
    # - display_name: Human-friendly display name (e.g., 'Optimism Mainnet')
    # - source_name: Name of the data source/loader (e.g., 'defillama', 'l2beat')
    # - source_rank: Numeric rank for source precedence in deduplication/merging
    REQUIRED_FIELDS = ["chain_id", "chain_name", "display_name", "source_name", "source_rank"]
    OPTIONAL_FIELDS = ["dt_day"]

    def __init__(self, **kwargs):
        self.config = kwargs
        log.debug("Initialized loader", loader=self.__class__.__name__, config=kwargs)

    @abc.abstractmethod
    def load_data(self, **kwargs) -> pl.DataFrame:
        """
        Load raw data from the source and return as a Polars DataFrame.
        Must be implemented by all loaders.
        """
        pass

    def validate_output(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Ensure DataFrame has all required fields.
        """
        missing = [f for f in self.REQUIRED_FIELDS if f not in df.columns]
        if missing:
            log.error(
                "Missing required fields in loader output",
                fields=missing,
                loader=self.__class__.__name__,
            )
            raise ValueError(f"Missing required fields: {missing}")
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
