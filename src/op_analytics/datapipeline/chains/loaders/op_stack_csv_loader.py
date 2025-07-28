import polars as pl
from op_analytics.datapipeline.chains.loaders.base import BaseChainMetadataLoader, register_loader


class GenericCsvChainMetadataLoader(BaseChainMetadataLoader):
    def __init__(self, csv_path: str, **kwargs):
        super().__init__(csv_path=csv_path, **kwargs)
        self.csv_path = csv_path  # Explicit for mypy

    def load_data(self) -> pl.DataFrame:
        df = pl.read_csv(self.csv_path)
        return self.add_metadata_columns(
            df=df, chain_key_col="chain_name", source="csv", source_rank=1
        )


register_loader("csv_loader", GenericCsvChainMetadataLoader)
