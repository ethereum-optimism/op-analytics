import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.schemas import (
    harmonize_to_canonical_schema,
    generate_chain_key,
)

log = structlog.get_logger()


class BaseChainMetadataLoader:
    """Base class for chain metadata loaders."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def run(self) -> pl.DataFrame:
        """Load and process chain metadata."""
        df = self.load_data()

        # Ensure chain_key exists
        if df.height > 0 and "chain_key" not in df.columns:
            raise ValueError(f"Loader {self.__class__.__name__} must add 'chain_key' column")

        return harmonize_to_canonical_schema(df)

    def add_metadata_columns(
        self, df: pl.DataFrame, chain_key_col: str, source: str, source_rank: int
    ) -> pl.DataFrame:
        """Helper method to safely add metadata columns, handling empty DataFrames."""
        if df.height == 0:
            return df
        return df.with_columns(
            [
                generate_chain_key(chain_key_col),
                pl.lit(source).alias("source"),
                pl.lit(source_rank).alias("source_rank"),
            ]
        )

    def load_data(self) -> pl.DataFrame:
        """Override this method to load data from your source."""
        raise NotImplementedError


# Simple registry
_LOADERS = {}


def register_loader(name: str, loader_class):
    """Register a loader class."""
    _LOADERS[name] = loader_class


def get_loader(name: str):
    """Get a loader class by name."""
    return _LOADERS.get(name)


def list_loaders():
    """List all registered loader names."""
    return list(_LOADERS.keys())


# For backward compatibility
class LoaderRegistry:
    @classmethod
    def register(cls, name: str, loader_cls):
        register_loader(name, loader_cls)

    @classmethod
    def get_loader(cls, name: str):
        return get_loader(name)

    @classmethod
    def list_loaders(cls):
        return list_loaders()
