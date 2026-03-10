from dataclasses import dataclass

import io
import polars as pl

from op_analytics.coreutils.request import new_session


@dataclass
class GrowThePieLabels:
    @classmethod
    def fetch(cls) -> pl.DataFrame:
        session = new_session()

        response = session.get("https://api.growthepie.xyz/v1/oli/labels_decoded.parquet")
        parquet_data = io.BytesIO(response.content)
        df = pl.read_parquet(parquet_data)

        return df
