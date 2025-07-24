import polars as pl
from op_analytics.coreutils.logger import structlog
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, LoaderRegistry
from op_analytics.datapipeline.chains.schemas import (
    harmonize_to_canonical_schema,
    generate_chain_key,
)
from op_analytics.datasources.l2beat.projects import L2BeatProjectsSummary

log = structlog.get_logger()


class L2BeatChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self, **kwargs) -> pl.DataFrame:
        log.info("Loading L2Beat chain metadata")
        meta = L2BeatProjectsSummary.fetch()
        df = meta.summary_df

        if df.height == 0:
            log.warning("L2Beat returned no data.")
            return harmonize_to_canonical_schema(pl.DataFrame())

        df = df.with_columns(
            [
                generate_chain_key("id"),
                pl.lit("l2beat").alias("source"),
                pl.lit(2).alias("source_rank"),
            ]
        )

        df = harmonize_to_canonical_schema(df)
        log.info(f"Loaded {len(df)} chains from L2Beat")
        return df


LoaderRegistry.register("l2beat_loader", L2BeatChainMetadataLoader)
