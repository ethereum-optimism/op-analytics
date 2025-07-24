import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import (
    harmonize_to_canonical_schema,
    generate_chain_key,
)

log = structlog.get_logger()


class GenericCsvChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self, **kwargs) -> pl.DataFrame:
        csv_path = kwargs.get("csv_path")
        if not isinstance(csv_path, str):
            raise ValueError(
                "csv_path (str) is required as a keyword argument for GenericCsvChainMetadataLoader"
            )
        log.info("Loading chain metadata from CSV", csv_path=csv_path)
        df = pl.read_csv(csv_path)

        if df.height == 0:
            log.warning(f"{csv_path} is empty.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        df = df.with_columns(
            [
                generate_chain_key("chain_name"),
                pl.lit("csv").alias("source"),
                pl.lit(1).alias("source_rank"),
            ]
        )

        df = harmonize_to_canonical_schema(df)
        log.info(f"Loaded {len(df)} chains from CSV")
        return df


LoaderRegistry.register("csv_loader", GenericCsvChainMetadataLoader)
