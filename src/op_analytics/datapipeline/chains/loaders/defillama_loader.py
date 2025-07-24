import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import (
    harmonize_to_canonical_schema,
    generate_chain_key,
)
from op_analytics.datasources.defillama.chaintvl.metadata import ChainsMetadata

log = structlog.get_logger()


class DefiLlamaChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Loading DefiLlama chain metadata")
        meta = ChainsMetadata.fetch()
        df = meta.df

        if df.height == 0:
            log.warning("DefiLlama returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        df = df.with_columns(
            [
                generate_chain_key("chain_name"),
                pl.lit("defillama").alias("source"),
                pl.lit(3).alias("source_rank"),
            ]
        )

        df = harmonize_to_canonical_schema(df)
        log.info(f"Loaded {len(df)} chains from DefiLlama")
        return df


LoaderRegistry.register("defillama_loader", DefiLlamaChainMetadataLoader)
