import polars as pl
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader
from op_analytics.datasources.defillama.chaintvl.metadata import ChainsMetadata


class DefiLlamaChainMetadataLoader(BaseChainMetadataLoader):
    def load_data(self) -> pl.DataFrame:
        df = ChainsMetadata.fetch().df
        return self.add_metadata_columns(
            df=df, chain_key_col="chain_name", source="defillama", source_rank=3
        )


register_loader("defillama_loader", DefiLlamaChainMetadataLoader)
