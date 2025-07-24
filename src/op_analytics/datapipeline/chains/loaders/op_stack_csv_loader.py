import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema

log = structlog.get_logger()


class GenericCsvChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from a user-specified CSV file.
    The CSV must contain columns: chain_name, defillama_slug, l2beat_slug, dune_schema, mainnet_chain_id, etc.
    Requires 'csv_path' as a keyword argument.
    """

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

        df = df.rename({"mainnet_chain_id": "chain_id"})

        df = df.with_columns(
            [
                pl.col("chain_name").alias("chain"),
                pl.coalesce(
                    pl.col("defillama_slug"),
                    pl.col("l2beat_slug"),
                    pl.col("dune_schema"),
                    pl.col("chain_name").str.to_lowercase().str.replace_all(" ", "-"),
                ).alias("chain_key"),
                pl.lit("csv_loader").alias("source_name"),
                pl.lit(0).cast(pl.Int32).alias("source_rank"),
            ]
        )

        return harmonize_to_canonical_schema(df)


LoaderRegistry.register("csv_loader", GenericCsvChainMetadataLoader)
