from dataclasses import dataclass

import polars as pl
import requests

from op_analytics.coreutils.env.vault import env_get
from op_analytics.coreutils.request import new_session, get_data
from op_analytics.coreutils.misc import raise_for_schema_mismatch

HISTORICAL_ENDPOINT = "https://api.daocpi.com/api/historic-cpi"

HISTORICAL_CPI_SCHEMA = {
    "date": pl.String(),
    "hhi": pl.Float64(),
    "cpi": pl.Float64(),
}


@dataclass
class HistoricalCPI:
    df: pl.DataFrame

    @classmethod
    def fetch(cls, session: requests.Session | None = None) -> "HistoricalCPI":
        session = session or new_session()

        api_key = env_get("DAOCPI_API_KEY")

        response = get_data(
            session,
            HISTORICAL_ENDPOINT,
            headers={"x-api-key": api_key},
        )

        breakpoint()

        rows = []
        for row in response:
            rows.append(
                {
                    "date": row["date"],
                    "hhi": row["HHI"],
                    "cpi": row["CPI"],
                }
            )
        df = pl.DataFrame(rows)

        raise_for_schema_mismatch(
            actual_schema=df.schema,
            expected_schema=pl.Schema(HISTORICAL_CPI_SCHEMA),
        )

        return cls(df=df)
