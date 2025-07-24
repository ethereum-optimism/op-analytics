import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import harmonize_to_canonical_schema
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()


class OpStackChainMetadataLoader(BaseChainMetadataLoader):
    """
    Loads chain metadata from the op_chains_tracking/inputs/chain_metadata_raw.csv file.
    """

    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Loading OP Stack chain metadata from local CSV")

        csv_path = repo_path("op_chains_tracking/inputs/chain_metadata_raw.csv")
        df = pl.read_csv(csv_path)

        if df.height == 0:
            log.warning("op_chains_tracking/inputs/chain_metadata_raw.csv is empty.")
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
                pl.lit("op_stack_csv").alias("source_name"),
                pl.lit(0).cast(pl.Int32).alias("source_rank"),
            ]
        )

        return harmonize_to_canonical_schema(df)


LoaderRegistry.register("op_stack_csv", OpStackChainMetadataLoader)
